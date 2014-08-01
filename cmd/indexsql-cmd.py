#!/usr/bin/env python

import sys, stat, time, os, errno
from bup import options, git, indexsql as indexmod, crawler
from bup.helpers import (
        handle_ctrl_c,
        parse_excludes,
        parse_rx_excludes,
        qprogress,
        saved_errors,
        )

def tuplize_path(path):
    drive, remainder = os.path.splitdrive(path)
    return (drive+os.sep,) + tuple(
            name for i, name in enumerate(path.split(os.sep)) if i)


def update_index(top, excluded_paths, exclude_rxs):
    top_tuple = tuplize_path(top)
    bup_dir = os.path.abspath(git.repo())

    index = indexmod.Index(indexfile)

    # shortcuts for convenience
    add_node = index.add_node
    update_node = index.update_node
    delete_node = index.delete_node

    index.add_ancestors(top_tuple)

    iiter = index.pre_order_iter(base=top_tuple)

    id, iname, idepth = next(iiter)

    fsiter = crawler.walk(
            top,
            xdev=opt.xdev,
            bup_dir=bup_dir,
            excluded_paths=excluded_paths,
            exclude_rxs=exclude_rxs,
            depths=[idepth],
            )

    fsname, fsdepth, st = next(fsiter)

    # pids records the ids of the fs tree as we iterate
    pids = {idepth: id}

    # we walk the union of the fs tree and index tree
    # and record the fs changes into the index
    inotdone = fsnotdone = True
    while inotdone and fsnotdone:
        if idepth < fsdepth:
            pids[fsdepth] = add_node(pids[fsdepth-1], fsname, st)
            fsupdate = True
        elif idepth == fsdepth:
            if iname < fsname:
                delete_node(id)
                iupdate = True
            elif iname == fsname:
                update_node(id, st)
                pids[idepth] = id
                fsupdate = iupdate = True
            else: # iname > fsname
                pids[fsdepth] = add_node(pids[fsdepth-1], fsname, st)
                fsupdate = True
        else: # idepth > fsdepth
            index.delete_node(id)
            iupdate = True

        if iupdate:
            try:
                id, iname, idepth = next(iiter)
            except StopIteration:
                inotdone = False
            else:
                iupdate = False

        if fsupdate:
            try:
                fsname, fsdepth, st = next(fsiter)
            except StopIteration:
                fsnotdone = False
            else:
                fsupdate = False

    if inotdone and not fsnotdone:
        # need to delete extra paths
        delete_node(id)
        for id, _, _ in iiter:
            delete_node(id)

    if fsnotdone and not inotdone:
        # need to add extra paths
        pids[fsdepth] = add_node(pids[fsdepth-1], fsname, st)
        for fsname, fsdepth, st in fsiter:
            pids[fsdepth] = add_node(pids[fsdepth-1], fsname, st)

optspec = """
bup index <-p|m|s|u> [options...] <filenames...>
--
 Modes:
p,print    print the index entries for the given names (also works with -u)
m,modified print only added/deleted/modified files (implies -p)
s,status   print each filename with a status char (A/M/D) (implies -p)
u,update   recursively update the index entries for the given file/dir names (default if no mode is specified)
check      carefully check index file integrity
clear      clear the default index
 Options:
H,hash     print the hash for each object next to its name
l,long     print more information about each file
no-check-device don't invalidate an entry if the containing device changes
fake-valid mark all index entries as up-to-date even if they aren't
fake-invalid mark all index entries as invalid
f,indexfile=  the name of the index file (normally BUP_DIR/bupindex)
exclude= a path to exclude from the backup (may be repeated)
exclude-from= skip --exclude paths in file (may be repeated)
exclude-rx= skip paths matching the unanchored regex (may be repeated)
exclude-rx-from= skip --exclude-rx patterns in file (may be repeated)
v,verbose  increase log output (can be used more than once)
x,xdev,one-file-system  don't cross filesystem boundaries
"""
option_parser = options.Options(optspec)
fatal = option_parser.fatal

(opt, flags, extra) = option_parser.parse(sys.argv[1:])

if not (opt.modified or
        opt['print'] or
        opt.status or
        opt.update or
        opt.check or
        opt.clear):
    opt.update = 1
if (opt.fake_valid or opt.fake_invalid) and not opt.update:
    fatal('--fake-{in,}valid are meaningless without -u')
if opt.fake_valid and opt.fake_invalid:
    fatal('--fake-valid is incompatible with --fake-invalid')
if opt.clear and opt.indexfile:
    fatal('cannot clear an external index (via -f)')

git.check_repo_or_die()
indexfile = opt.indexfile or git.repo('bupindex.sqlite')

handle_ctrl_c()

if opt.check:
    log('check: starting initial check.\n')
    # What needs to be done here?

if opt.clear:
    log('clear: clearing index.\n')
    try:
        os.remove(indexfile)
        if opt.verbose:
            log('clear: removed %s\n' % path)
    except OSError as err:
        if err.errno != errno.enoent:
            raise


excluded_paths = parse_excludes(flags, fatal)
exclude_rxs = parse_rx_excludes(flags, fatal)
paths = indexmod.reduce_paths(extra)

if opt.update:
    if not extra:
        fatal('update mode (-u) requested but no paths given')
    for realpath, path in paths:
        update_index(realpath, excluded_paths, exclude_rxs)

if opt['print'] or opt.status or opt.modified:
    pathd = dict()
    index = indexmod.ReadOnlyIndex(indexfile)
    for id, name, depth in index.pre_order_iter():
        pathd[depth] = name
        print os.path.join(*(pathd[i] for i in range(depth+1)))
    #raise NotImplementedError

if opt.check and (opt['print'] or opt.status or opt.modified or opt.update):
    log('check: starting final check.\n')
    # What needs to be done here?

if saved_errors:
    log('WARNING: %d errors encountered.\n' % len(saved_errors))
    sys.exit(1)
