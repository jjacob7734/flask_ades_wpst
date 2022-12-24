##############################################################################
# ADES / WPS-T Flask App
#
# Implementation of an Algorithm Deployment and Execution Service (ADES) that
# is compliant with a subset of the OGC Web Processing Service - Transactional
# (WPS-T) specification.  Web service calls can be used to deploy, undeploy,
# or get information about a process (job or container type and version), and
# to execute, dismiss, get status, and get results of jobs for a particular
# process.
#
# Run this script with the "[-h] [--help]" option to get usage information,
# and query the root endpoint like "http://<host>:<port>/" (default
# "http://127.0.0.1:5000/") to get a landing page showing information about
# the available endpoints.
##############################################################################

import argparse
from flask import Flask, request, Response
import json
import os
from flask_ades_wpst.ades_base import ADES_Base
import hashlib
from socket import getfqdn
from datetime import datetime


# ADES Settings
#
# Set the version of the JSON response API used.  This version number will
# automatically be included as part of every JSON response.  Client codes
# can check this version to determine what keywords and structure are
# expected from each WPS-T endpoint.
#
# The recommended process for updating this API version is as follows.
# Update this setting everytime the JSON structure of any endpoint response
# is changed.  If this is in a development branch in between public releases,
# modify the part to the right of the decimal point.  In preparing for a new
# release, if the part to the right of the decimal point is "0" (meaning no
# change in API since the last release), then leave this  unchanged.  If the
# part to the right of the decimal point is not "0", then increment the part
# to the left of the decimal point and set the part to the right of the
# decimal point to "0".
API_VERSION = "1.0"

# Set the default host and port for the Flask app.
DEFAULT_FLASK_HOST = "127.0.0.1"
DEFAULT_FLASK_PORT = 5000
DEFAULT_FLASK_DEBUG = False

# List of the valid ADES Platform settings
ADES_PLATFORM_OPTIONS = ( "Generic", "K8s", "PBS" )

# Flask app object
app = Flask(__name__)


def default_ades_id():
    '''Generate a default ADES identifier.
    '''
    # Create a hash string using the hostname and current date-time and use 
    # it in the default ADES ID to make it unique.
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")
    hostname = getfqdn()
    return "-".join(["ades",
                     hashlib.sha1((hostname + now).encode()).hexdigest()])

def parse_args():
    '''Parse command line arguments.
    '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-d", "--debug", action="store_true",
                        help="turn on Flask debug mode")
    parser.add_argument("-H", "--host", default="127.0.0.1",
                        help="host IP address for Flask server")
    parser.add_argument("-p", "--port", default=5000,
                        help="host port number for Flask server")
    parser.add_argument("-n", "--name", 
                        help="ID of this ADES instance")
    args = parser.parse_args()
    return args.debug, args.host, args.port, args.name

def ades_resp(d):
    '''Inject additional elements into every endpoint response.
    '''
    # Add additional global elements to the Flask endpoint return dictionary.
    d |= { "ades_id": app.config["ADES_ID"],
           "api_version": API_VERSION }
    return d

# [GET] getLandingPage
@app.route("/", methods = ['GET'])
def root():
    resp_dict = { "landingPage": { "links": [
        { "href": "/", "type": "GET", "title": "getLandingPage",
          "parameters": "", "payload": "",
          "example": "curl http://127.0.0.1:5000/" },
        { "href": "/processes", "type": "GET", "title": "getProcesses",
          "parameters": "", "payload": "",
          "example": "curl http://127.0.0.1:5000/processes" },
        { "href": "/processes", "type": "POST", "title": "deployProcess",
          "parameters": "proc=<url-to-app.json>", "payload": "",
          "example": "curl -X POST http://127.0.0.1:5000/processes/proc=https://public-url/to-your-application-descriptor.json" },
        { "href": "/processes/<procID>", "type": "GET",
          "title": "getProcessDescription", "parameters": "", "payload": "",
          "example": "curl http://127.0.0.1:5000/processes/<your-process-id-from-getProcesses>" },
        { "href": "/processes/<procID>", "type": "DELETE",
          "title": "undeployProcess", "parameters": "", "payload": "",
          "example": "curl -X DELETE http://127.0.0.1:5000/processes/<your-process-id-from-getProcesses>" },
        { "href": "/processes/<procID>/jobs", "type": "GET",
          "title": "getJobList", "parameters": "", "payload": "",
          "example": "curl http://127.0.0.1:5000/processes/<your-process-id-from-getProcesses>/jobs" },
        { "href": "/processes/<procID>/jobs", "type": "POST",
          "title": "execute", "parameters": "user=<username>",
          "payload": "<workflow-inputs>",
          "example": "curl -H \"Content-Type: application/json\" -X POST -d '{\"param1\"=\"value1\", \"param2\"=\"value2\"}' http://127.0.0.1:5000/processes/<your-process-id-from-getProcesses>/jobs" },
        { "href": "/processes/<procID>/jobs/<jobID>", "type": "GET",
          "title": "getStatus", "parameters": "", "payload": "",
          "example": "curl http://127.0.0.1:5000/processes/<your-process-id-from-getProcesses>/jobs/<your-job-id-from-getJobList>" },
        { "href": "/processes/<procID>/jobs/<jobID>", "type": "DELETE",
          "title": "dismiss", "parameters": "", "payload": "",
          "example": "curl -X DELETE http://127.0.0.1:5000/processes/<your-process-id-from-getProcesses>/jobs/<your-job-id-from-getJobList>" },
        { "href": "/processes/<procID>/jobs/<jobID>/result", "type": "GET",
          "title": "getResult", "parameters": "", "payload": "",
          "example": "curl http://127.0.0.1:5000/processes/<your-process-id-from-getProcesses>/jobs/<your-job-id-from-getJobList>/result" }]}}
    status_code = 200
    return (ades_resp(resp_dict), 
            status_code, {'ContentType':'application/json'})

# [GET] getProcesses
# [POST] deployProcess
@app.route("/processes", methods = ['GET', 'POST'])
def processes():
    resp_dict = {}
    status_code = 200
    ades_base = ADES_Base(app.config)
    if request.method == 'GET':
        # Retrieve list of processes that have been deployed.
        proc_list = ades_base.get_procs()
        resp_dict = {"processes": proc_list}
    elif request.method == 'POST':
        # Deploy a new process.
        req_vals = request.values
        proc_info = ades_base.deploy_proc(req_vals["proc"])
        resp_dict = {"deploymentResult": {"processSummary": proc_info}}
        status_code = 201
    return (ades_resp(resp_dict), 
            status_code, {'ContentType':'application/json'})

# [GET] getProcessDescription
# [DELETE] undeployProcess
@app.route("/processes/<procID>", methods = ['GET', 'DELETE'])
def processes_id(procID):
    resp_dict = {}
    status_code = 200
    ades_base = ADES_Base(app.config)
    if request.method == 'GET':
        # Describe a process.
        resp_dict = {"process": ades_base.get_proc(procID)}
    elif request.method == "DELETE":
        # Undeploy a process.
        resp_dict = {"undeploymentResult": ades_base.undeploy_proc(procID)}
    return (ades_resp(resp_dict), 
            status_code, {'ContentType':'application/json'})

# [GET] getJobList
# [POST] executeJob
@app.route("/processes/<procID>/jobs", methods = ['GET', 'POST'])
def processes_jobs(procID):
    ades_base = ADES_Base(app.config)
    if request.method == 'GET':
        # Get the list of jobs for a given process.
        status_code = 200
        job_list = ades_base.get_jobs(procID)
        resp_dict = {"jobs": job_list}
    elif request.method == 'POST':
        # Execute a new job.
        status_code = 201
        job_params = request.get_json()
        req_vals = request.values
        job_info = ades_base.exec_job(procID, job_params, req_vals)
        resp_dict = job_info
    return (ades_resp(resp_dict), 
            status_code, {'ContentType':'application/json'})

# [GET] getJobStatus
# [DELETE] dismissJob
@app.route("/processes/<procID>/jobs/<jobID>", methods = ['GET', 'DELETE'])
def processes_job(procID, jobID):
    status_code = 200
    ades_base = ADES_Base(app.config)
    if request.method == 'GET':
        # Get status of a given job.
        resp_dict = {"statusInfo": ades_base.get_job(procID, jobID)}
    elif request.method == 'DELETE':
        # Dismiss a job.
        dismiss_status = ades_base.dismiss_job(procID, jobID)
        resp_dict = {"statusInfo": dismiss_status}
        if not dismiss_status:
            # OGC specs prescribe a status code 404 Not Found for this
            # request to dismiss a job that doesn't exist.
            status_code = 404 
    return (ades_resp(resp_dict), 
            status_code, {'ContentType':'application/json'})
    
# [GET] getJobResults
@app.route("/processes/<procID>/jobs/<jobID>/result", methods = ['GET'])
def processes_result(procID, jobID):
    # Get links to the results of a job.
    status_code = 200
    ades_base = ADES_Base(app.config)
    resp_dict = ades_base.get_job_results(procID, jobID)
    return (ades_resp(resp_dict), 
            status_code, {'ContentType':'application/json'})

def flask_wpst(app, ades_id, debug=DEFAULT_FLASK_DEBUG,
               host=DEFAULT_FLASK_HOST, port=DEFAULT_FLASK_PORT,
               valid_platforms=ADES_PLATFORM_OPTIONS):
    # Register ADES ID from value provided on the command line or one that
    # was auto-generated.
    if ades_id is None:
        ades_id = default_ades_id()
    app.config["ADES_ID"] = ades_id

    # Get the ADES home directory from the environment and ensure it exists
    ades_home = os.environ.get("ADES_HOME", default="./ades")
    if not os.path.exists(ades_home):
        raise OSError("ADES_HOME path {} does not exist.".\
                         format(ades_home))
    app.config["ADES_HOME"] = ades_home

    # Get the platform setting (e.g., PBS, K8s) from the environment and
    # validate it..
    platform = os.environ.get("ADES_PLATFORM", default="Generic")
    if platform not in valid_platforms:
        raise ValueError("ADES_PLATFORM invalid - {} not in {}.".\
                         format(platform, valid_platforms))
    app.config["PLATFORM"] = platform

    # Start listening on the registered WPS-T endpoints.
    app.run(debug=debug, host=host, port=port)
    

if __name__ == "__main__":
    # Parse command line arguments.
    debug_mode, flask_host, flask_port, ades_id = parse_args()

    # Start Flask app.
    flask_wpst(app, ades_id,
               debug=debug_mode, host=flask_host, port=flask_port)
