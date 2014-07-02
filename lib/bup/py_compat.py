import sys

PY_MAJOR_VERSION = sys.version_info.major

# code is from six.py
if PY_MAJOR_VERSION < 3:
    def exec_(_code_, _globs_=None, _locs_=None):
        """Execute code in a namespace."""
        if _globs_ is None:
            frame = sys._getframe(1)
            _globs_ = frame.f_globals
            if _locs_ is None:
                _locs_ = frame.f_locals
            del frame
        elif _locs_ is None:
            _locs_ = _globs_
        exec("""exec _code_ in _globs_, _locs_""")

    exec_("""
def reraise(error, traceback):
    raise type(error), error, traceback
        """)
else:
    import builtins
    exec_ = getattr(builtins, 'exec')

    def reraise(error, traceback):
        if error.__traceback__ is traceback:
            raise error
        raise error.with_traceback(traceback)

if PY_MAJOR_VERSION < 3:
    from cStringIO import StringIO
else:
    from io import StringIO # probably actually want BytesIO

if PY_MAJOR_VERSION < 3:
    from urllib import unquote as urlunquote
else:
    from urllib.parse import unquote as urlunquote

if PY_MAJOR_VERSION < 3:
    import cPickle as pickle
else:
    import pickle

def iter(cls):
    py2_next = getattr(cls, 'next', None)
    py3_next = getattr(cls, '__next__', None)
    if py2_next is None and py3_next is not None:
        setattr(cls, 'next', py3_next)
    if py3_next is None and py2_next is not None:
        setattr(cls, '__next__', py2_next)
    return cls
