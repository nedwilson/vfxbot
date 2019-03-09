#!/usr/local/bin/python

import sys
import os

sys.path.insert(0, r'/Volumes/romeo_inhouse/romeo/SHARED/lib/python')
sys.path.insert(0, r'/Volumes/romeo_inhouse/romeo/SHARED/shotgun/install/core/python')
sys.path.insert(0, r'/Volumes/romeo_inhouse/romeo/SHARED/lib/nuke/pipeline')
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
os.environ['IH_SHOW_CFG_PATH'] = r'/Volumes/romeo_inhouse/romeo/SHARED/romeo/lib/romeo.cfg'
os.environ['IH_SHOW_CODE'] = r'romeo'

from vfxbot import app as application