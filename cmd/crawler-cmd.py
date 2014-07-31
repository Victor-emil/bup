#!/usr/bin/env python

from os.path import relpath
from bup import options, crawler
from bup.helpers import *

optspec = """
bup drecurse <path>
--
x,xdev,one-file-system   don't cross filesystem boundaries
exclude= a path to exclude from the backup (can be used more than once)
exclude-from= a file that contains exclude paths (can be used more than once)
exclude-rx= skip paths matching the unanchored regex (may be repeated)
exclude-rx-from= skip --exclude-rx patterns in file (may be repeated)
q,quiet  don't actually print filenames
profile  run under the python profiler
"""
o = options.Options(optspec)
(opt, flags, extra) = o.parse(sys.argv[1:])

excluded_paths = parse_excludes(flags, o.fatal)
exclude_rxs = parse_rx_excludes(flags, o.fatal)
it = crawler.recursive_dirlist(extra, opt.xdev,
                                excluded_paths=excluded_paths,
                                exclude_rxs=exclude_rxs)
if opt.profile:
    import cProfile
    def do_it():
        for i in it:
            pass
    cProfile.run('do_it()')
else:
    if opt.quiet:
        for i in it:
            pass
    else:
        for (name,st) in it:
            print name

if saved_errors:
    log('WARNING: %d errors encountered.\n' % len(saved_errors))
    sys.exit(1)
