import collections
import itertools
import os
import sqlite3

from bup import helpers, py_compat, xstat

EMPTY_SHA = b'\0'*20

IX_EXISTS = 0x8000        # file exists on filesystem
IX_HASHVALID = 0x4000     # the stored sha1 matches the filesystem
IX_SHAMISSING = 0x2000    # the stored sha1 object doesn't seem to exist

class ReadOnlyIndex(object):
    def __init__(self, indexfile, _skip_check = False):
        if not _skip_check and not os.path.isfile(indexfile):
            raise ValueError("no index at '%s'" % indexfile)

        self.connection = sqlite3.connect(indexfile)

    def _ancestors(self, path):
        pathiter = iter(path)
        drive = next(pathiter)

        values = tuple(
                itertools.chain(
                    itertools.chain(*enumerate(pathiter)),
                    (drive,)
                    )
                )

        depth = (len(values)-1)/2

        # create a cursor which gets the closest ancestor
        cursor = self.connection.cursor()
        cursor.execute("""
            WITH RECURSIVE
                path_list(depth, name) AS
                    (VALUES {values}),
                path(id, depth) AS (
                    SELECT nodes.id, 0
                        FROM edges, nodes
                        WHERE
                            edges.parent IS NULL AND
                            edges.child=nodes.id AND
                            nodes.name=?
                    UNION ALL
                    SELECT nodes.id, path.depth+1
                        FROM nodes, edges, path, path_list
                        WHERE
                            edges.parent=path.id AND
                            edges.child=nodes.id AND
                            path_list.depth=path.depth AND
                            path_list.name=nodes.name
                )
            SELECT * FROM path ORDER BY 2 DESC
            """.format(values=', '.join(
                pair for pair in itertools.repeat('(?, ?)', depth))),
            values,
            )

        return depth, cursor

    def ancestors(self, path):
        return self._ancestors(path)[1]

    def closest_ancestor(self, path):
        return next(self.ancestors(path), (None, -1))

    def get(self, path):
        orig_depth, ancestors = self._ancestors(path)
        closest, depth = next(ancestors, (None, -1))
        if depth == orig_depth:
            return closest, depth
        else:
            return None, -1

    def _pre_order_iter(self, base=None):
        if base is None:
            depth = 0
            base_table = """
            SELECT edges.child, nodes.name, ?
                FROM edges, nodes
                WHERE
                    edges.parent IS NULL AND
                    edges.child=nodes.id
            """
            values = (depth, )
        else:
            id, depth = self.get(base)
            if id is None:
                raise ValueError("'%s' is not in index" % os.path.join(base))
            base_table = "VALUES (?, ?, ?)"
            values = (id, base[-1], depth)

        # setup cursor to yield Rows
        cursor = self.connection.cursor()

        # populate a cursor with the dfs traversal
        cursor.execute("""
            WITH RECURSIVE
                dfs(id, name, depth) AS (
                    {base_table}
                    UNION ALL
                    SELECT  edges.child,
                            nodes.name,
                            dfs.depth+1
                        FROM edges, nodes, dfs
                        WHERE
                            edges.child=nodes.id AND
                            edges.parent=dfs.id
                        ORDER BY 3 DESC, 2 ASC
                )
            SELECT * FROM dfs
            """.format(base_table=base_table),
            values,
            )

        return depth, cursor

    def pre_order_iter(self, base=None):
        return self._pre_order_iter(base)[1]

    def post_order_iter(self, base=None):
        base_depth, pre_order_iter = self._pre_order_iter(base)

        # initialize the last depth
        last_depth = base_depth-1

        # setup stack to follow the pre-order traversal
        stack = collections.deque() # marginally faster than a list

        # make a few aliases to remove attribute lookups
        pop = stack.pop
        push = stack.append
        repeat = itertools.repeat

        # unpack the pre-order traversal as post-order
        for row in pre_order_iter:
            depth = row[-1]
            if depth <= last_depth:
                yield pop()
                if depth < last_depth:
                    # technically the conditional is not necessary, but in practice
                    # directories have a decent number of files, so gating off the
                    # extra function call (and empty-iterator construction) seems
                    # like a good idea

                    # itertools.repeat is by far the fastest way to do something
                    # a specified number of times
                    for _ in repeat(None, last_depth-depth):
                        yield pop()
            push(row)
            last_depth = depth
        while stack:
            yield pop()

class Index(ReadOnlyIndex):

    def __init__(self, indexfile):
        super(Index, self).__init__(indexfile, True)

        try:
            self._create_tables()
        except sqlite3.OperationalError:
            # not a new database
            pass

    def __del__(self):
        self.connection.commit()

    def _create_tables(self):
        cursor = self.connection.cursor()
        cursor.execute("""
            CREATE TABLE edges (
                parent  INTEGER,
                child   INTEGER,
                PRIMARY KEY(child)
                )
            """)
        cursor.execute("CREATE INDEX parent_idx ON edges (parent)")
        cursor.execute("""
            CREATE TABLE nodes (
                id      INTEGER,
                name    BLOB,
                info_id INTEGER,
                PRIMARY KEY(id)
                )
            """)
        cursor.execute("""
            CREATE TABLE info (
                id      INTEGER,
                mode    INTEGER,
                ino     INTEGER,
                dev     INTEGER,
                nlink   INTEGER,
                uid     INTEGER,
                gid     INTEGER,
                size    INTEGER,
                ctime   INTEGER,
                mtime   INTEGER,
                atime   INTEGER,
                PRIMARY KEY(id)
                )
            """)

    def add_node(self, pid, name, st):
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO info
                (mode, ino,  dev,   nlink, uid,
                 gid,  size, atime, mtime, ctime)
                VALUES
                    (?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?)
            """,
            (st.st_mode, st.st_ino,  st.st_dev,   st.st_nlink, st.st_uid,
             st.st_gid,  st.st_size, st.st_atime, st.st_mtime, st.st_ctime),
        )
        cursor.execute(
            "INSERT INTO nodes (name, info_id) VALUES (?, ?)",
            (name, cursor.lastrowid),
            )
        id = cursor.lastrowid
        cursor.execute(
            "INSERT INTO edges VALUES (?, ?)",
            (pid, id),
            )
        return id

    def add_down_to_root(self, path):
        id, depth = self.closest_ancestor(path)
        for i in range(depth+1, len(path)):
            id = self.add_node(
                    id, path[i], xstat.lstat(os.path.join(*path[:i+1])))

    def update_node(self, id, st):
        cursor = self.connection.cursor()
        cursor.execute("""
            UPDATE info
                SET
                    mode=?, ino=?,  dev=?,   nlink=?, uid=?,
                    gid=?,  size=?, atime=?, mtime=?, ctime=?
                WHERE info.id IN (
                    SELECT info_id
                        FROM nodes
                        WHERE nodes.id=?
                    )
            """,
            (st.st_mode, st.st_ino,  st.st_dev,   st.st_nlink, st.st_uid,
             st.st_gid,  st.st_size, st.st_atime, st.st_mtime, st.st_ctime,
             id),
            )

    def delete_node(self, id):
        cursor = self.connection.cursor()
        cursor.execute("""
            DELETE FROM edges
                WHERE child=?
            """,
            (id,),
            )
        cursor.execute("""
            DELETE FROM info
                WHERE info.id IN (
                    SELECT info_id
                        FROM nodes
                        WHERE nodes.id=?
                    )
            """,
            (id,),
            )
        cursor.execute("""
            DELETE FROM nodes
                WHERE nodes.id=?
            """,
            (id,),
            )

def reduce_paths(paths):
    paths = dict(
            (os.path.normpath(helpers.realpath(path)), path) for path in paths)
    res = []
    for realpath, path in paths.items():
        if not any(realpath.startswith(base+os.sep) for base in paths):
            res.append((realpath, path))
    res.sort()
    return res
