import argparse
from flask import Flask, request, Response
import json
from flask_ades_wpst.ades_base import get_procs, get_proc, deploy_proc, undeploy_proc, get_jobs, get_job, exec_job, dismiss_job, get_job_results

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
    if request.method == 'GET':
        # Retrieve available processes
        proc_list = get_procs()
        resp_dict = {"processes": proc_list}
    elif request.method == 'POST':
        req_vals = request.values
        proc_info = deploy_proc(req_vals["proc"])
        resp_dict = {"deploymentResult": {"processSummary": proc_info}}
    return resp_dict, status_code, {'ContentType':'application/json'}

@app.route("/processes/<procID>", methods = ['GET', 'DELETE'])
def processes_id(procID):
    resp_dict = {}
    status_code = 200
    if request.method == 'GET':
        resp_dict = {"process": get_proc(procID)}
    elif request.method == "DELETE":
        resp_dict = {"undeploymentResult": undeploy_proc(procID)}
    return resp_dict, status_code, {'ContentType':'application/json'}

@app.route("/processes/<procID>/jobs", methods = ['GET', 'POST'])
def processes_jobs(procID):
    if request.method == 'GET':
        status_code = 200
        job_list = get_jobs()
        resp_dict = {"jobs": job_list}
    elif request.method == 'POST':
        status_code = 201
        req_vals = request.values
        job_info = exec_job(req_vals["job"])
#        resp_dict = {"Location": status_url}
        resp_dict = job_info
    return resp_dict, status_code, {'ContentType':'application/json'}

@app.route("/processes/<procID>/jobs/<jobID>", methods = ['GET', 'DELETE'])
def processes_job(procID, jobID):
    status_code = 200
    if request.method == 'GET':
        resp_dict = {"statusInfo": get_job(procID, jobID)}
    elif request.method == 'DELETE':
        dismiss_status = dismiss_job(procID, jobID)
        resp_dict = {"statusInfo": dismiss_status}
    return resp_dict, status_code, {'ContentType':'application/json'}
    
@app.route("/processes/<procID>/jobs/<jobID>/result", methods = ['GET'])
def processes_result(procID, jobID):
    job_outputs = get_job_results(procID, jobID)
    job_outputs_json = [{"href": jout} for jout in job_outputs]
    resp_dict = {"result":
                 {"outputInfo":
                  {"id": jobID, "outputs": job_outputs_json}}}
    status_code = 200
    return resp_dict, status_code, {'ContentType':'application/json'}

def flask_wpst(app, debug=False, host="127.0.0.1"):
    app.run(debug=debug, host=host)
    

if __name__ == "__main__":
    host = parse_args()
    flask_wpst(app, debug=True, host=host)
