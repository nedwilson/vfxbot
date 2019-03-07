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

# globals

g_log = None
# Number of process threads to run
g_num_threads = 6
g_process_queue = Queue.Queue()
g_config = None
g_nuke_exe_path = None

def init_logging():
    global g_log
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

def globals_from_config():
    global g_config, g_nuke_exe_path
    config_file = os.environ['IH_SHOW_CFG_PATH']
    g_config = ConfigParser.ConfigParser()
    g_config.read(config_file)
    g_nuke_exe_path = g_config.get('nuke_exe_path', sys.platform)

def _lut_convert(m_logger_object, m_data):
    global g_nuke_exe_path
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
        pass
        # os.remove(path)
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
        m_process_queue.task_done()

VERSION = 'v0.0.1'
init_logging()
globals_from_config()
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
    filepath = request.json['filepath']
    destination_lut_format = request.json['destination_lut_format']
    if not os.path.exists(filepath):
        abort(404, 'Unable to locate %s on the filesystem.'%filepath)
    filebase = os.path.splitext(filepath)[0]
    converted_lut = '.'.join([filebase, destination_lut_format])
    vfxbot_request = {'type' : 'lut_convert', 'data' : {'source_lut_file': filepath, 'destination_lut_file' : converted_lut, 'destination_lut_format' : destination_lut_format}}
    g_process_queue.put(vfxbot_request)
    return jsonify(vfxbot_request['data']), 200

if __name__ == '__main__':
    # app.run(debug=True)
    app.run()
