"""
This package stores all files used for data retrieval, processing, saving and loading
"""
import os as _os
import sys as _sys

_proj_dir = _os.path.abspath(_os.path.dirname(_os.path.dirname(__file__)))
_os.chdir(_proj_dir)
_sys.path.append(_os.path.join(_proj_dir, 'data'))

import pandasutils
import loader
import fields
