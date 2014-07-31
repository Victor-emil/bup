import sys

PY_MAJOR_VERSION = sys.version_info.major

if PY_MAJOR_VERSION < 3:
    import itertools

    imap = itertools.imap
    izip_longest = itertools.izip_longest
else:
    import itertools, builtins

    imap = builtins.map
    izip_longest = itertools.zip_longest

if PY_MAJOR_VERSION < 3:
    import __builtin__

    basestring = __builtin__.basestring
else:
    import builtins

    basestring = (bultins.bytes, builtins.str)
