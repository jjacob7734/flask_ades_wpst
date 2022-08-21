import argparse
from flask import Flask, request, Response
import json
import os
from flask_ades_wpst.ades_base import ADES_Base
import hashlib
from socket import getfqdn
from datetime import datetime


app = Flask(__name__)

def default_ades_id():
    # Create a hash string using the hostname and current date-time and use 
    # it in the default ADES ID to make it unique.
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")
    hostname = getfqdn()
    return "-".join(["ades",
                     hashlib.sha1((hostname + now).encode()).hexdigest()])

def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-d", "--debug", action="store_true")
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
    # Add ADES ID to the Flask endpoint return dictionary.
    d |= { "ades-id": app.config["ADES_ID"] }
    return d

@app.route("/", methods = ['GET'])
def root():
    resp_dict = {"landingPage": {"links": [
        {"href": "/", "type": "GET", "title": "getLandingPage"},
        {"href": "/processes", "type": "GET", "title": "getProcesses"},
        {"href": "/processes", "type": "POST", "title": "deployProcess"},
        {"href": "/processes/<procID>", "type": "GET",
         "title": "getProcessDescription"},
        {"href": "/processes/<procID>", "type": "DELETE",
         "title": "undeployProcess"},
        {"href": "/processes/<procID>/jobs", "type": "GET",
         "title": "getJobList"},
        {"href": "/processes/<procID>/jobs", "type": "POST",
         "title": "execute"},
        {"href": "/processes/<procID>/jobs/<jobID>", "type": "GET",
         "title": "getStatus"},
        {"href": "/processes/<procID>/jobs/<jobID>", "type": "DELETE",
         "title": "dismiss"},
        {"href": "/processes/<procID>/jobs/<jobID>/result", "type": "GET",
         "title": "getResult"}]}}
    status_code = 200
    return (ades_resp(resp_dict), 
            status_code, {'ContentType':'application/json'})

@app.route("/processes", methods = ['GET', 'POST'])
def processes():
    resp_dict = {}
    status_code = 200
    ades_base = ADES_Base(app.config)
    if request.method == 'GET':
        # Retrieve available processes
        proc_list = ades_base.get_procs()
        resp_dict = {"processes": proc_list}
    elif request.method == 'POST':
        req_vals = request.values
        proc_info = ades_base.deploy_proc(req_vals["proc"])
        resp_dict = {"deploymentResult": {"processSummary": proc_info}}
        status_code = 201
    return (ades_resp(resp_dict), 
            status_code, {'ContentType':'application/json'})

@app.route("/processes/<procID>", methods = ['GET', 'DELETE'])
def processes_id(procID):
    resp_dict = {}
    status_code = 200
    ades_base = ADES_Base(app.config)
    if request.method == 'GET':
        resp_dict = {"process": ades_base.get_proc(procID)}
    elif request.method == "DELETE":
        resp_dict = {"undeploymentResult": ades_base.undeploy_proc(procID)}
    return (ades_resp(resp_dict), 
            status_code, {'ContentType':'application/json'})

@app.route("/processes/<procID>/jobs", methods = ['GET', 'POST'])
def processes_jobs(procID):
    ades_base = ADES_Base(app.config)
    if request.method == 'GET':
        status_code = 200
        job_list = ades_base.get_jobs(procID)
        resp_dict = {"jobs": job_list}
    elif request.method == 'POST':
        status_code = 201
        job_params = request.get_json()
        job_info = ades_base.exec_job(procID, job_params)
        resp_dict = job_info
    return (ades_resp(resp_dict), 
            status_code, {'ContentType':'application/json'})

@app.route("/processes/<procID>/jobs/<jobID>", methods = ['GET', 'DELETE'])
def processes_job(procID, jobID):
    status_code = 200
    ades_base = ADES_Base(app.config)
    if request.method == 'GET':
        resp_dict = {"statusInfo": ades_base.get_job(procID, jobID)}
    elif request.method == 'DELETE':
        dismiss_status = ades_base.dismiss_job(procID, jobID)
        resp_dict = {"statusInfo": dismiss_status}
        if not dismiss_status:
            # OGC specs prescribe a status code 404 Not Found for this
            # request to dismiss a job that doesn't exist.
            status_code = 404 
    return (ades_resp(resp_dict), 
            status_code, {'ContentType':'application/json'})
    
@app.route("/processes/<procID>/jobs/<jobID>/result", methods = ['GET'])
def processes_result(procID, jobID):
    status_code = 200
    ades_base = ADES_Base(app.config)
    resp_dict = ades_base.get_job_results(procID, jobID)
    return (ades_resp(resp_dict), 
            status_code, {'ContentType':'application/json'})

def flask_wpst(app, debug=False, host="127.0.0.1", port=5000,
               valid_platforms = ("Generic", "K8s", "PBS")):
    platform = os.environ.get("ADES_PLATFORM", default="Generic")
    if platform not in valid_platforms:
        raise ValueError("ADES_PLATFORM invalid - {} not in {}.".\
                         format(platform, valid_platforms))
    app.config["PLATFORM"] = platform
    app.run(debug=debug, host=host, port=port)
    

if __name__ == "__main__":
    print ("starting")
    debug_mode, flask_host, flask_port, ades_id = parse_args()
    if ades_id is None:
        ades_id = default_ades_id()
    app.config["ADES_ID"] = ades_id
    flask_wpst(app, debug=debug_mode, host=flask_host, port=flask_port)
