#!/usr/local/bin/python

from flask import Flask, jsonify, make_response, request, abort
import logging
import os
import sys
import Queue
from threading import Thread
import ConfigParser
import tempfile
import subprocess
import db_access as DB
import re
import glob
import traceback

# globals

g_log = None
# Number of process threads to run
g_num_threads = 6
g_process_queue = Queue.Queue()
g_config = None
g_nuke_exe_path = None
g_show_code = None
g_inhouse_project_id = -1
g_production_project_id = -1
g_shot_scope_regexp = None
g_sequence_scope_regexp = None
g_show_scope_regexp = None
g_imgseq_regexp = re.compile(r'(^.*\.)(#+|%[0-9]+d)(\..*$)')
g_frame_regexp = re.compile(r'(^.*\.)([0-9]+)(\..*$)')
g_image_extensions = []
g_production_shot_tree = None
g_inhouse_shot_tree = None

g_ihdb = None
g_proddb = None

def init_logging():
    global g_log, g_ihdb, g_proddb
    homedir = os.path.expanduser('~')
    logfile = ""
    if sys.platform == 'win32':
        logfile = os.path.join(homedir, 'AppData', 'Local', 'IHPipeline', '%s.log' % 'vfxbot')
    elif sys.platform == 'darwin':
        logfile = os.path.join(homedir, 'Library', 'Logs', 'IHPipeline', '%s.log' % 'vfxbot')
    elif sys.platform == 'linux2':
        logfile = os.path.join(homedir, 'Logs', 'IHPipeline', '%s.log' % 'vfxbot')
    if not os.path.exists(os.path.dirname(logfile)):
        os.makedirs(os.path.dirname(logfile))
    logFormatter = logging.Formatter("%(asctime)s:[%(threadName)s]:[%(levelname)s]:%(message)s")
    g_log = logging.getLogger()
    g_log.setLevel(logging.INFO)
    try:
        devmode = os.environ['NUKE_DEVEL']
        g_log.setLevel(logging.DEBUG)
    except:
        pass
    fileHandler = logging.FileHandler(logfile)
    fileHandler.setFormatter(logFormatter)
    g_log.addHandler(fileHandler)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    g_log.addHandler(consoleHandler)
    g_ihdb.set_logger_object(g_log)
    g_proddb.set_logger_object(g_log)

def globals_from_config():
    global g_config, g_nuke_exe_path, g_show_code, g_shot_scope_regexp, g_sequence_scope_regexp, g_show_scope_regexp, \
        g_production_project_id, g_ihdb, g_proddb, g_image_extensions, g_production_shot_tree, g_inhouse_shot_tree, \
        g_inhouse_project_id
    config_file = os.environ['IH_SHOW_CFG_PATH']
    g_show_code = os.environ['IH_SHOW_CODE']
    g_config = ConfigParser.ConfigParser()
    g_config.read(config_file)
    g_nuke_exe_path = g_config.get('nuke_exe_path', sys.platform)
    g_shot_scope_regexp = re.compile(g_config.get('vfxbot', 'shot_scope_regexp_%s'%sys.platform))
    g_sequence_scope_regexp = re.compile(g_config.get('vfxbot', 'sequence_scope_regexp_%s'%sys.platform))
    g_show_scope_regexp = re.compile(g_config.get('vfxbot', 'show_scope_regexp_%s'%sys.platform))
    g_production_project_id = int(g_config.get('database', 'shotgun_production_project_id'))
    g_image_extensions = g_config.get('vfxbot', 'image_extensions').split(',')
    g_production_shot_tree = g_config.get('vfxbot', 'production_shot_tree')
    g_inhouse_shot_tree = g_config.get('vfxbot', 'inhouse_shot_tree')
    g_inhouse_project_id = int(g_config.get('database', 'shotgun_project_id'))
    g_ihdb = DB.DBAccessGlobals.get_db_access()
    g_proddb = DB.DBAccessGlobals.get_db_access()
    g_proddb.set_project_id(g_production_project_id)


def _lut_convert(m_logger_object, m_data):
    global g_nuke_exe_path
    b_overwrite = True
    try:
        b_overwrite = m_data['overwrite']
    except KeyError:
        m_logger_object.info('Boolean overwrite has not been set in the request object. Defaulting to True.')

    if b_overwrite == False:
        m_logger_object.info('Boolean overwrite set to False in request.')
        if os.path.exists(m_data['destination_lut_file']):
            m_logger_object.info('Converted LUT file already exists at %s. Since overwrite is set to False, this will be skipped.'%m_data['destination_lut_file'])
            return
    else:
        m_logger_object.info('Boolean overwrite is set to True, will proceed even if file exists at destination.')
    m_logger_object.info('Converting LUT at %s'%m_data['source_lut_file'])
    fd, path = tempfile.mkstemp(suffix='.py')
    m_logger_object.info('Temporary Python script: %s'%path)
    nuke_py_interpreter = os.path.join(os.path.dirname(g_nuke_exe_path), 'python')
    nuke_cmd_list = [g_nuke_exe_path, '-t', path]
    try:
        with os.fdopen(fd, 'w') as tmp:
            tmp.write('#!%s\n\n'%nuke_py_interpreter)
            tmp.write('import nuke\n\n')
            tmp.write('cmstp_node = nuke.Node("CMSTestPattern")\n')
            tmp.write('ociofile_node = nuke.Node("OCIOFileTransform")\n')
            tmp.write('ociofile_node.connectInput(0, cmstp_node)\n')
            tmp.write('ociofile_node.knob("file").setValue("%s")\n'%m_data['source_lut_file'])
            tmp.write('ociofile_node.knob("maskChannelMask").setValue("none")\n')
            tmp.write('genlut_node = nuke.Node("GenerateLUT")\n')
            tmp.write('genlut_node.knob("file_type").setValue(".%s")\n'%m_data['destination_lut_format'])
            tmp.write('genlut_node.knob("file").setValue("%s")\n'%m_data['destination_lut_file'])
            tmp.write('genlut_node.connectInput(0, ociofile_node)\n')
            tmp.write('genlut_node.knob("generate").execute()\n')

        m_logger_object.info('About to execute: %s'%' '.join(nuke_cmd_list))
        nuke_sp = subprocess.Popen(' '.join(nuke_cmd_list), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        while nuke_sp.poll() == None:
            m_logger_object.info(nuke_sp.stdout.readline().strip())
        if nuke_sp.poll() != 0:
            m_logger_object.error('Something went wrong with Nuke.')
    finally:
        os.remove(path)
    m_logger_object.info('Done.')

def process_vfxbot_request(m_logger_object, m_process_queue):
    m_logger_object.info('VFXBot Process Request thread initialized.')
    while True:
        request = m_process_queue.get()
        request_type = request['type']
        request_data = request['data']
        m_logger_object.info('Received request %s.'%request_type)
        if request_type == 'lut_convert':
            _lut_convert(m_logger_object, request_data)
        elif request_type == 'transcode_plate':
            _transcode_plate(m_logger_object, request_data, request['db_version_object'], request['db_connection_object'])
        m_process_queue.task_done()

def _transcode_plate(m_logger_object, request_data, db_version_object, db_connection_object):
    global g_nuke_exe_path, g_config, g_show_code
    m_logger_object.debug('Inside _transcode_plate()')
    m_logger_object.debug('Destination Version Object:')
    m_logger_object.debug(db_version_object)
    b_overwrite = True
    try:
        b_overwrite = request_data['overwrite']
    except KeyError:
        m_logger_object.info('Boolean overwrite has not been set in the request object. Defaulting to True.')

    if b_overwrite == False:
        m_logger_object.info('Boolean overwrite set to False in request.')
        if db_version_object.g_start_frame == -1:
            tmp_file_path = db_version_object.g_path_to_frames
        else:
            tmp_file_path = db_version_object.g_path_to_frames%db_version_object.g_start_frame
        if os.path.exists(tmp_file_path):
            m_logger_object.info('Transcoded plate already exists at %s. Since overwrite is set to False, this will be skipped.'%tmp_file_path)
            return
    else:
        m_logger_object.info('Boolean overwrite is set to True, will proceed even if file exists at destination.')
    m_logger_object.info('Transcoding plate at %s'%db_version_object.g_path_to_frames)
    fd, path = tempfile.mkstemp(suffix='.py')
    m_logger_object.info('Temporary Python script: %s'%path)
    nuke_py_interpreter = os.path.join(os.path.dirname(g_nuke_exe_path), 'python')
    nuke_cmd_list = [g_nuke_exe_path, '-t', path]
    s_delivery_template = g_config.get('delivery', 'nuke_template_%s'%sys.platform)
    try:
        with os.fdopen(fd, 'w') as tmp:
            tmp.write('#!%s\n\n'%nuke_py_interpreter)
            tmp.write('import nuke\n\n')
            tmp.write("import os\n")
            tmp.write("import sys\n")
            tmp.write("\n")
            if sys.platform == 'win32':
                s_delivery_template = s_delivery_template.replace('\\', '\\\\')
            tmp.write("nuke.scriptOpen(\"%s\")\n" % s_delivery_template)
            tmp.write("nd_root = root = nuke.root()\n")
            tmp.write("\n")
            tmp.write("# set root frame range\n")
            tmp.write("nd_root.knob('first_frame').setValue(%d)\n" % db_version_object.g_start_frame)
            tmp.write("nd_root.knob('last_frame').setValue(%d)\n" % db_version_object.g_end_frame)
            tmp.write("nd_root.knob('nocc').setValue(False)\n")
            tmp.write("\n")
            tmp.write("nd_read = nuke.toNode('%s')\n" % g_config.get('delivery', 'exr_read_node'))
            tmp.write("nd_read.knob('file').setValue(\"%s\")\n" % request_data['source_filepath'])
            tmp.write("nd_read.knob('first').setValue(%d)\n" % db_version_object.g_start_frame)
            tmp.write("nd_read.knob('last').setValue(%d)\n" % db_version_object.g_end_frame)
            tmp.write("nd_root.knob('ti_ih_file_name').setValue(\"%s\")\n" % db_version_object.g_version_code)
            tmp.write("nd_root.knob('ti_ih_sequence').setValue(\"%s\")\n" % request_data['sequence'])
            tmp.write("nd_root.knob('ti_ih_shot').setValue(\"%s\")\n" % request_data['shot'])
            tmp.write("nd_root.knob('ti_ih_version').setValue(\"%s\")\n" % '-1')
            tmp.write("nd_root.knob('ti_ih_vendor').setValue(\"%s\")\n" % 'VFXBot')
            tmp.write("nd_root.knob('ti_submission_reason').setValue(\"%s\")\n" % 'Scan Check')
            tmp.write("nd_root.knob('ti_ih_notes_1').setValue(\"%s\")\n" % db_version_object.g_description)
            tmp.write("nd_root.knob('txt_ih_show').setValue(\"%s\")\n" % os.path.basename(request_data['show_root']))
            tmp.write("nd_root.knob('txt_ih_show_path').setValue(\"%s\")\n" % request_data['show_root'])
            tmp.write("nd_root.knob('txt_ih_seq').setValue(\"%s\")\n" % request_data['sequence'])
            tmp.write("nd_root.knob('txt_ih_seq_path').setValue(\"%s\")\n" % g_config.get(g_show_code, 'seq_dir_format').format(pathsep = os.path.sep,
                                                                                                                                show_root = request_data['show_root'],
                                                                                                                                sequence = request_data['sequence']))
            tmp.write("nd_root.knob('txt_ih_shot').setValue(\"%s\")\n" % request_data['shot'])
            tmp.write("nd_root.knob('txt_ih_shot_path').setValue(\"%s\")\n" % g_config.get(g_show_code, 'shot_dir_format').format(pathsep = os.path.sep,
                                                                                                                                  show_root = request_data['show_root'],
                                                                                                                                  sequence = request_data['sequence'],
                                                                                                                                  shot = request_data['shot']))
            tmp.write("nd_root.knob('exportburnin').setValue(True)\n")
            for shotlutnode in g_config.get('delivery', 'shot_lut_nodes').split(','):
                if len(shotlutnode) > 0:
                    tmp.write("nuke.toNode('%s').knob('file').setValue(\"%s\")\n" % (shotlutnode, request_data['lut_file']))
            l_exec_nodes = []
            # Avid Quicktime Write Node
            tmp.write("nuke.toNode('%s').knob('disable').setValue(False)\n" % g_config.get('delivery', 'avid_write_node'))
            tmp.write("nuke.toNode('%s').knob('file').setValue('%s')\n" % (
            g_config.get('delivery', 'avid_write_node'), db_version_object.g_path_to_dnxhd))
            l_exec_nodes.append(g_config.get('delivery', 'avid_write_node'))
            # VFX Quicktime Write Node
            tmp.write("nuke.toNode('%s').knob('disable').setValue(False)\n" % g_config.get('delivery', 'vfx_write_node'))
            tmp.write("nuke.toNode('%s').knob('file').setValue('%s')\n" % (
            g_config.get('delivery', 'vfx_write_node'), db_version_object.g_path_to_movie))
            l_exec_nodes.append(g_config.get('delivery', 'vfx_write_node'))
            # Export Quicktime Write Node
            tmp.write("nuke.toNode('%s').knob('disable').setValue(False)\n" % g_config.get('delivery', 'export_write_node'))
            tmp.write("nuke.toNode('%s').knob('file').setValue('%s')\n" % (
            g_config.get('delivery', 'export_write_node'), db_version_object.g_path_to_export))
            l_exec_nodes.append(g_config.get('delivery', 'export_write_node'))
            # EXR Write Node
            tmp.write("nuke.toNode('%s').knob('disable').setValue(False)\n" % g_config.get('delivery', 'hires_write_node'))
            tmp.write("nuke.toNode('%s').knob('file').setValue('%s')\n" % (
            g_config.get('delivery', 'hires_write_node'), db_version_object.g_path_to_frames))
            l_exec_nodes.append(g_config.get('delivery', 'hires_write_node'))
            # Turn off DI Mattes for this
            tmp.write("nuke.toNode('%s').knob('disable').setValue(True)\n" % g_config.get('delivery', 'matte_write_node'))
            s_exec_nodes = (', '.join('nuke.toNode("' + write_node + '")' for write_node in l_exec_nodes))
            # Let's make a thumbnail
            thumb_relative_path = g_config.get('thumbnails', 'shot_thumb_dir').format(pathsep = os.path.sep)
            thumb_write_path = os.path.join(request_data['delivery_base_dir'], thumb_relative_path, '%s_thumbnail.%%04d.png'%db_version_object.g_version_code)
            tmp.write("nuke.toNode('%s').knob('file').setValue('%s')\n" % (
                g_config.get('delivery', 'thumbnail_write_node'),thumb_write_path))
            thumb_frame = ((db_version_object.g_end_frame - db_version_object.g_start_frame)/2)+db_version_object.g_start_frame
            tmp.write("nuke.toNode('%s').knob('disable').setValue(False)\n" % g_config.get('delivery', 'thumbnail_write_node'))
            tmp.write("nuke.execute(nuke.toNode(\"%s\"),%d,%d,1,)\n" % (g_config.get('delivery', 'thumbnail_write_node'), thumb_frame, thumb_frame))
            tmp.write("nuke.executeMultiple((%s),((%d,%d,1),))\n" % (s_exec_nodes, db_version_object.g_start_frame - 1, db_version_object.g_end_frame))

        m_logger_object.info('About to execute: %s'%' '.join(nuke_cmd_list))
        nuke_sp = subprocess.Popen(' '.join(nuke_cmd_list), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        while nuke_sp.poll() == None:
            m_logger_object.info(nuke_sp.stdout.readline().strip())
        if nuke_sp.poll() != 0:
            m_logger_object.error('Errors occurred in Nuke Render.')
            raise Exception('Errors have occurred in Nuke Render.')
        m_logger_object.info('Transcode render is successful!')
        if not request_data['transcode_only']:
            m_logger_object.info('Updating Database')
            if db_version_object.g_dbid == -1:
                m_logger_object.info('Transcoded Version is new, will be created in the database.')
                db_connection_object.create_version(db_version_object)
            else:
                m_logger_object.info('Transcoded Version has ID of %d, will be updated in the database.'%db_version_object.g_dbid)
                db_connection_object.update_version(db_version_object)
            # upload the thumbnail
            thumb_frame_path = thumb_write_path%thumb_frame
            m_logger_object.info('Uploading thumbnail: %s'%thumb_frame_path)
            db_connection_object.upload_thumbnail('Version', db_version_object, thumb_frame_path)
            m_logger_object.info('Uploading movie: %s'%db_version_object.g_path_to_export)
            db_connection_object.upload_movie('Version', db_version_object, db_version_object.g_path_to_export)
    except:
        m_logger_object.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        os.remove(path)

    m_logger_object.info('Done.')

VERSION = 'v0.0.1'
globals_from_config()
init_logging()
for i in range(g_num_threads):
    worker = Thread(target=process_vfxbot_request, args=(g_log, g_process_queue))
    worker.setDaemon(True)
    worker.start()


app = Flask(__name__)

@app.route('/')
def index():
    return make_response(jsonify({'info' : 'VFXBot API, Version %s'%VERSION}))

@app.errorhandler(400)
def handle_error_400(error):
    return jsonify({'error' : error.description}), 400

@app.errorhandler(404)
def handle_error_404(error):
    return jsonify({'error' : error.description}), 404

@app.route('/vfxbot/lut_convert', methods=['POST'])
def lut_convert():
    global g_process_queue
    if not request.json:
        abort(400, 'Malformed or non-existant request.')
    if not 'filepath' in request.json:
        abort(400, 'filepath must be provided in POST request.')
    if not 'destination_lut_format' in request.json:
        abort(400, 'destination_lut_format must be provided in POST request.')
    b_overwrite = True
    if 'overwrite' in request.json:
        if request.json['overwrite'] == 'False':
            b_overwrite = False
        elif request.json['overwrite'] == 'True':
            b_overwrite = True
        else:
            abort(400, 'Value for request parameter overwrite "%s" is invalid. Valid values are "True" or "False".'%request.json['overwrite'])
    filepath = request.json['filepath']
    # is this an in-house plate or a production plate?

    destination_lut_format = request.json['destination_lut_format']
    if not os.path.exists(filepath):
        abort(404, 'Unable to locate %s on the filesystem.'%filepath)
    filebase = os.path.splitext(filepath)[0]
    converted_lut = '.'.join([filebase, destination_lut_format])
    vfxbot_request = {'type' : 'lut_convert', 'data' : {'source_lut_file': filepath, 'destination_lut_file' : converted_lut, 'destination_lut_format' : destination_lut_format, 'overwrite' : b_overwrite}}
    g_process_queue.put(vfxbot_request)
    return jsonify(vfxbot_request['data']), 200

@app.route('/vfxbot/transcode_plate', methods=['POST'])
def transcode_plate():
    global g_process_queue, g_config, g_ihdb, g_proddb, g_log, g_imgseq_regexp, g_frame_regexp, g_shot_scope_regexp, \
        g_sequence_scope_regexp, g_show_scope_regexp, g_image_extensions, g_production_shot_tree, g_inhouse_shot_tree, \
        g_production_project_id, g_inhouse_project_id
    if not request.json:
        abort(400, 'Malformed or non-existant request.')
    if not 'filepath' in request.json:
        abort(400, 'filepath must be provided in POST request.')
    b_overwrite = True
    if 'overwrite' in request.json:
        if request.json['overwrite'] == 'False':
            b_overwrite = False
        elif request.json['overwrite'] == 'True':
            b_overwrite = True
        else:
            abort(400, 'Value for request parameter overwrite "%s" is invalid. Valid values are "True" or "False".'%request.json['overwrite'])
    filepath = request.json['filepath']
    fileext = os.path.splitext(filepath)[-1][1:]
    if fileext not in g_image_extensions:
        abort(400, 'File extension provided, %s, is not in the list of valid file extensions. Valid file extensions are %s.'%(fileext, str(g_image_extensions)))
    g_log.debug('Inside transcode plate. Using filepath %s.'%filepath)
    imgseq_match = g_imgseq_regexp.search(filepath)
    imgseq_files = []
    b_file_not_found = False
    first_frame = -1
    last_frame = -1
    duration = 1
    if imgseq_match:
        g_log.debug('%s is an image sequence.'%filepath)
        imgseq_glob = re.sub(g_imgseq_regexp, r'\1*\3', filepath)
        imgseq_files = sorted(glob.glob(imgseq_glob))
        if len(imgseq_files) == 0:
            b_file_not_found = True
        else:
            first_frame = int(imgseq_files[0].split('.')[-2])
            last_frame = int(imgseq_files[-1].split('.')[-2])
            duration = last_frame - first_frame + 1
    else:
        if not os.path.exists(filepath):
            b_file_not_found = True
    # Error 404 if item(s) to transcode do not exist
    if b_file_not_found:
        abort(404, 'Unable to locate %s on the filesystem.'%filepath)
    # figure out the scope
    shot = None
    dbshot = None
    sequence = None
    dbsequence = None
    shot_tree = None
    b_show_level = False
    b_transcode_only = False
    project_id = -1
    db = None
    shot_match = g_shot_scope_regexp.search(filepath)
    show_root = None
    delivery_base_dir = None
    if shot_match:
        shot = shot_match.groupdict()['shot']
        sequence = shot_match.groupdict()['sequence']
        shot_tree = shot_match.groupdict()['shottree']
    else:
        sequence_match = g_sequence_scope_regexp.search(filepath)
        if sequence_match:
            sequence = sequence_match.groupdict()['sequence']
            shot_tree = sequence_match.groupdict()['shottree']
        else:
            show_match = g_show_scope_regexp.search(filepath)
            if show_match:
                b_show_level = True
                shot_tree = show_match.groupdict()['shottree']
            else:
                b_transcode_only = True
    if shot_tree:
        show_match = g_show_scope_regexp.search(filepath)
        show_root = show_match.group(0)
        if shot_tree == g_inhouse_shot_tree:
            db = g_ihdb
            project_id = g_inhouse_project_id
        elif shot_tree == g_production_shot_tree:
            db = g_proddb
            project_id = g_production_project_id
        else:
            g_log.warning('Somehow we are located on a recognized, mounted filesystem, but not part of a production or In-House shot tree.')
            g_log.warning('filepath: %s, shot_tree: %s'%(filepath, shot_tree))
            b_transcode_only = True

    filebase = os.path.basename(filepath)
    filedir = os.path.dirname(filepath)

    if shot and show_root:
        delivery_base_dir = g_config.get(g_show_code, 'shot_dir_format').format(show_root = show_root, pathsep = os.path.sep, sequence = sequence, shot = shot)
    elif sequence and show_root:
        delivery_base_dir = g_config.get(g_show_code, 'seq_element_dir_format').format(show_root=show_root,
                                                                                pathsep=os.path.sep, sequence = sequence)
    elif show_root:
        delivery_base_dir = g_config.get(g_show_code, 'show_element_dir').format(show_root=show_root,
                                                                                pathsep=os.path.sep)
    else:
        delivery_base_dir = filedir

    # get information from the database for versions and entities if we are not set to transcode_only

    version_name = filebase.split('.')[0]
    file_format = g_config.get('delivery', 'file_format')
    avid_file_format = g_config.get('delivery', 'avid_file_format')
    vfx_file_format = g_config.get('delivery', 'vfx_file_format')
    export_file_format = g_config.get('delivery', 'export_file_format')
    shot_delivery_folder = g_config.get('delivery', 'shot_delivery_folder')
    avidqt_dest = g_config.get('delivery', 'avidqt_dest')
    vfxqt_dest = g_config.get('delivery', 'vfxqt_dest')
    exportqt_dest = g_config.get('delivery', 'exportqt_dest')
    hires_dest = g_config.get('delivery', 'hires_dest')
    frame_padding = g_config.get(g_show_code, 'write_frame_format')
    hires_destination_path = os.path.join(delivery_base_dir, shot_delivery_folder, hires_dest.format(pathsep = os.path.sep, format = fileext, frame = frame_padding, client_version = version_name, hiresext = file_format))
    avidqt_destination_path = os.path.join(delivery_base_dir, shot_delivery_folder, avidqt_dest.format(pathsep = os.path.sep, client_version = version_name, avidqtext = avid_file_format))
    vfxqt_destination_path = os.path.join(delivery_base_dir, shot_delivery_folder, vfxqt_dest.format(pathsep = os.path.sep, client_version = version_name, vfxqtext = vfx_file_format))
    exportqt_destination_path = os.path.join(delivery_base_dir, shot_delivery_folder, exportqt_dest.format(pathsep = os.path.sep, client_version = version_name, exportqtext = export_file_format))
    transcode_version_obj = DB.Version('%s_ScanCheck'%version_name, -1, 'Transcode by VFXBot', first_frame, last_frame,
                                               duration, hires_destination_path, vfxqt_destination_path, None, None, None)
    transcode_version_obj.set_path_to_dnxhd(avidqt_destination_path)
    transcode_version_obj.set_path_to_export(exportqt_destination_path)
    transcode_version_obj.set_version_type('Comp')
    transcode_version_obj.set_delivered(True)
    transcode_version_obj.set_status(g_config.get('delivery', 'db_delivered_status'))

    lut_file = g_config.get(g_show_code, 'default_cc_%s'%sys.platform)
    shot_lut_file_ext = g_config.get(g_show_code, 'cdl_file_ext')
    shot_lut_file_path = g_config.get(g_show_code, 'cdl_dir_format').format(pathsep = os.path.sep)

    if not b_transcode_only:
        source_version_obj = None
        dest_version_obj = None
        entity_type = 'Project'
        entity_id = project_id
        transcode_version_obj.set_version_entity({ 'type' : entity_type, 'id' : entity_id })
        if shot:
            dbshot = db.fetch_shot(shot)
            entity_type = 'Shot'
            entity_id = dbshot.g_dbid
            transcode_version_obj.set_version_entity({'type': entity_type, 'id': entity_id})
            tmp_lut_file_path = os.path.join(delivery_base_dir, shot_lut_file_path, '%s.%s'%(version_name, shot_lut_file_ext))
            if not os.path.exists(tmp_lut_file_path):
                g_log.warning('Plate to transcode is inside a shot, but no LUT file exists at %s. Using the default.'%tmp_lut_file_path)
            else:
                lut_file = tmp_lut_file_path
                g_log.info('Using LUT file at %s.'%lut_file)
        else:
            if sequence:
                dbsequence = db.fetch_sequence(sequence)
                entity_type = 'Sequence'
                entity_id = dbsequence.g_dbid
                transcode_version_obj.set_version_entity({'type': entity_type, 'id': entity_id})
        # now, get list of versions matching the name and for the specific entity
        dbversions = db.fetch_versions_for_entity('%s_ScanCheck'%version_name, entity_type, entity_id)
        if len(dbversions) == 0:
            g_log.warning('No Versions found in database for entity type %s, entity ID %d, and Version Name %s'%(entity_type, entity_id, version_name))
        else:
            for dbversion in dbversions:
                if dbversion.g_version_type == 'Scan':
                    source_version_obj = dbversion
                elif dbversion.g_version_type == 'Comp':
                    dest_version_obj = dbversion
                    transcode_version_obj.g_dbid = dest_version_obj.g_dbid

    vfxbot_request = {'type' : 'transcode_plate', 'data' : {'source_filepath': filepath, 'destination_version_id' : str(transcode_version_obj.g_dbid),
                                                            'destination_filepath' : transcode_version_obj.g_path_to_frames,
                                                            'overwrite' : b_overwrite, 'transcode_only' : b_transcode_only,
                                                            'lut_file' : lut_file, 'show_root' : show_root, 'sequence' : sequence, 'shot' : shot,
                                                            'delivery_base_dir' : delivery_base_dir },
                      'db_version_object' : transcode_version_obj,
                      'db_connection_object' : db }
    g_process_queue.put(vfxbot_request)
    return jsonify(vfxbot_request['data']), 200

if __name__ == '__main__':
    # app.run(debug=True)
    app.run()
