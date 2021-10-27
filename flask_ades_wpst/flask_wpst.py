import argparse
from flask import Flask, request, Response
import json
import os
from flask_ades_wpst.ades_base import ADES_Base

def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-H", "--host", default="127.0.0.1",
                        help="host IP address for Flask server")
    args = parser.parse_args()
    return args.host

app = Flask(__name__)

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
    return resp_dict, status_code, {'ContentType':'application/json'}

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
    return resp_dict, status_code, {'ContentType':'application/json'}

@app.route("/processes/<procID>", methods = ['GET', 'DELETE'])
def processes_id(procID):
    resp_dict = {}
    status_code = 200
    ades_base = ADES_Base(app.config)
    if request.method == 'GET':
        resp_dict = {"process": ades_base.get_proc(procID)}
    elif request.method == "DELETE":
        resp_dict = {"undeploymentResult": ades_base.undeploy_proc(procID)}
    return resp_dict, status_code, {'ContentType':'application/json'}

@app.route("/processes/<procID>/jobs", methods = ['GET', 'POST'])
def processes_jobs(procID):
    ades_base = ADES_Base(app.config)
    if request.method == 'GET':
        status_code = 200
        job_list = ades_base.get_jobs()
        resp_dict = {"jobs": job_list}
    elif request.method == 'POST':
        status_code = 201
        job_params = request.get_json()
        job_info = ades_base.exec_job(procID, job_params)
        resp_dict = job_info
    return resp_dict, status_code, {'ContentType':'application/json'}

@app.route("/processes/<procID>/jobs/<jobID>", methods = ['GET', 'DELETE'])
def processes_job(procID, jobID):
    status_code = 200
    ades_base = ADES_Base(app.config)
    if request.method == 'GET':
        resp_dict = {"statusInfo": ades_base.get_job(procID, jobID)}
    elif request.method == 'DELETE':
        dismiss_status = ades_base.dismiss_job(procID, jobID)
        resp_dict = {"statusInfo": dismiss_status}
    return resp_dict, status_code, {'ContentType':'application/json'}
    
@app.route("/processes/<procID>/jobs/<jobID>/result", methods = ['GET'])
def processes_result(procID, jobID):
    status_code = 200
    ades_base = ADES_Base(app.config)
    resp_dict = ades_base.get_job_results(procID, jobID)
    return resp_dict, status_code, {'ContentType':'application/json'}

def flask_wpst(app, debug=False, host="127.0.0.1",
               valid_platforms = ("Generic", "K8s", "PBS")):
    platform = os.environ.get("ADES_PLATFORM", default="Generic")
    if platform not in valid_platforms:
        raise ValueError("ADES_PLATFORM invalid - {} not in {}.".\
                         format(platform, valid_platforms))
    app.config["PLATFORM"] = platform
    app.run(debug=debug, host=host)
    

if __name__ == "__main__":
    print ("starting")
    host = parse_args()
    flask_wpst(app, debug=True, host=host)
