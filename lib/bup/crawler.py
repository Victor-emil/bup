import stat, os
from bup import py_compat
from bup.xstat import lstat
from bup.helpers import debug1, should_rx_exclude_path
from errno import EINVAL, ELOOP, ENOENT
from os import curdir, listdir, open, close, fchdir
from os.path import join

try:
    O_LARGEFILE = os.O_LARGEFILE
except AttributeError:
    O_LARGEFILE = 0
try:
    O_NOFOLLOW = os.O_NOFOLLOW
except AttributeError:
    O_NOFOLLOW = 0

OPEN_MASK = os.O_RDONLY | O_LARGEFILE | O_NOFOLLOW | os.O_NDELAY

# the use of fchdir() and lstat() is for two reasons:
#  - help out the kernel by not making it repeatedly look up the absolute path
#  - avoid race conditions caused by doing listdir() on a changing symlink

def compute_depth(path):
    normpath = os.path.normpath(path)
    _, remainder = os.path.splitdrive(normpath)
    remainder = remainder.split(os.sep)
    depth = len(remainder)-1
    if len(remainder) > 1 and not remainder[1]: # trailing os.sep
        depth -= 1
    return depth

def _iterdir():
    try:
        names = sorted(listdir(curdir))
    except OSError as e:
        if e.errno != EINVAL:
            raise
        # the directory got deleted after we cded into it
        names = ()
    for name in names:
        try:
            yield name, lstat(name)
        except OSError as e:
            if e.errno != ENOENT:
                raise

def _walk(
        base,
        pfd,
        dev=None,
        bup_dir=None,
        excluded_paths=None,
        exclude_rxs=None,
        fullpaths=False,
        depth=0,
        ):
    for name, st in _iterdir():
        path = join(base, name)

        if excluded_paths:
            if os.path.normpath(path) in excluded_paths:
                debug1('Skipping %r: excluded.\n' % path)
                continue

        if exclude_rxs and should_rx_exclude_path(path, exclude_rxs):
            continue

        if stat.S_ISDIR(st.st_mode):
            if bup_dir is not None:
                if os.path.normpath(path) == bup_dir:
                    debug1('Skipping BUP_DIR.\n')
                    continue

            if fullpaths:
                yield path, depth, st
            else:
                yield name, depth, st

            if dev is not None and st.st_dev != dev:
                debug1('Skipping contents of %r: different filesystem.\n' % path)
                continue

            try:
                fd = open(name, OPEN_MASK)
            except OSError as e:
                if e.errno not in (ELOOP, ENOENT):
                    raise
                continue

            try:
                fchdir(fd)
                for x in _walk(
                        base=path,
                        pfd=fd,
                        dev=dev,
                        bup_dir=bup_dir,
                        excluded_paths=excluded_paths,
                        exclude_rxs=exclude_rxs,
                        fullpaths=fullpaths,
                        depth=depth+1,
                        ):
                    yield x
            finally:
                close(fd)

            fchdir(pfd)

        else:
            if fullpaths:
                yield path, depth, st
            else:
                yield name, depth, st

def walk(
        paths,
        xdev=None,
        bup_dir=None,
        excluded_paths=None,
        exclude_rxs=None,
        fullpaths=False,
        depths=None,
        ):

    if isinstance(paths, py_compat.basestring):
        paths = (paths,)

    if depths is None:
        depths = iter(())
    else:
        depths = iter(depths)

    startfd = open(curdir, OPEN_MASK)

    try:
        for path in paths:
            try:
                st = lstat(path)
            except OSError as e:
                if e.errno != ENOENT:
                    raise
                continue

            try:
                depth = next(depths)
            except StopIteration:
                depth = compute_depth(path)

            if fullpaths:
                name = path
            else:
                name = os.path.basename(path)

            if stat.S_ISDIR(st.st_mode):
                if bup_dir is not None:
                    if os.path.normpath(path) == bup_dir:
                        debug1('Skipping BUP_DIR.\n')
                        continue

                yield name, depth, st

                dev = st.st_dev if xdev else None

                try:
                    fd = open(path, OPEN_MASK)
                except OSError as e:
                    if e.errno not in (ELOOP, ENOENT):
                        raise
                    continue

                try:
                    fchdir(fd)
                    for x in _walk(
                            base=path,
                            pfd=fd,
                            dev=dev,
                            bup_dir=bup_dir,
                            excluded_paths=excluded_paths,
                            exclude_rxs=exclude_rxs,
                            fullpaths=fullpaths,
                            depth=depth+1,
                            ):
                        yield x
                finally:
                    close(fd)

                fchdir(startfd)

            else:
                yield name, depth, st

    except:
        try:
            if startfd is not None:
                fchdir(startfd)
        except:
            pass
        raise
    finally:
        close(startfd)
