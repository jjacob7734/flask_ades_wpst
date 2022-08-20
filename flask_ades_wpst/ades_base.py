import sys
import requests
import json
import hashlib
from flask_ades_wpst.sqlite_connector import (
    sqlite_get_procs,
    sqlite_get_proc,
    sqlite_deploy_proc,
    sqlite_undeploy_proc,
    sqlite_get_jobs,
    sqlite_get_job,
    sqlite_exec_job,
    sqlite_dismiss_job,
    sqlite_update_job_status,
)
from datetime import datetime


class ADES_Base:
    def __init__(self, app_config):
        self._app_config = app_config
        self._platform = app_config["PLATFORM"]
        if self._platform == "Generic":
            from flask_ades_wpst.ades_generic import ADES_Generic as ADES_Platform
        elif self._platform == "K8s":
            from flask_ades_wpst.ades_k8s import ADES_K8s as ADES_Platform
        elif self._platform == "PBS":
            from flask_ades_wpst.ades_pbs import ADES_PBS as ADES_Platform
        else:
            # Invalid platform setting.  If you do implement a new
            # platform here, you must also add it to the valid_platforms
            # tuple default argument to the flask_wpst function in
            # flask_wpst.py.
            raise ValueError("Platform {} not implemented.".format(self._platform))
        self._ades = ADES_Platform()
        self._ades_id = app_config["ADES_ID"]

    def proc_dict(self, proc):
        return {
            "id": proc[0],
            "title": proc[1],
            "abstract": proc[2],
            "keywords": proc[3],
            "owsContextURL": proc[4],
            "processVersion": proc[5],
            "jobControlOptions": proc[6].split(","),
            "outputTransmission": proc[7].split(","),
            "immediateDeployment": str(bool(proc[8])).lower(),
            "executionUnit": proc[9],
        }

    def get_procs(self):
        saved_procs = sqlite_get_procs()
        procs = [self.proc_dict(saved_proc) for saved_proc in saved_procs]
        return procs

    def get_proc(self, proc_id):
        proc_desc = sqlite_get_proc(proc_id)
        return self.proc_dict(proc_desc)

    def deploy_proc(self, proc_desc_url):
        response = requests.get(proc_desc_url)
        if response.status_code == 200:
            proc_spec = response.json()

            # generate proc_id
            proc_desc = proc_spec["processDescription"]
            proc_desc2 = proc_desc["process"]
            proc_id = f"{proc_desc2['id']}-{proc_desc['processVersion']}"

            # overwrite the process ID
            proc_desc2["id"] = proc_id

            sqlite_deploy_proc(proc_spec)
            ades_resp = self._ades.deploy_proc(proc_spec)
        return proc_spec

    def undeploy_proc(self, proc_id):
        proc_desc = sqlite_undeploy_proc(proc_id)
        if proc_desc:
            proc_desc = self.proc_dict(proc_desc)
            print("proc_desc: ", proc_desc)
            ades_resp = self._ades.undeploy_proc(proc_desc)
        return proc_desc

    def get_jobs(self, proc_id=None):
        jobs = sqlite_get_jobs(proc_id)
        return jobs

    def get_job(self, proc_id, job_id):
        # Required fields in job_info response dict:
        #   jobID (str)
        #   status (str) in ["accepted" | "running" | "succeeded" | "failed"]
        # Optional fields:
        #   expirationDate (dateTime)
        #   estimatedCompletion (dateTime)
        #   nextPoll (dateTime)
        #   percentCompleted (int) in range [0, 100]
        job_spec = sqlite_get_job(job_id)

        # bypass querying the ADES backend if status is either:
        # dismissed, successful, or failed
        job_info = {
            "jobID": job_id,
            "status": job_spec["status"],
            "metrics": job_spec["metrics"],
        }
        if job_spec["status"] in ("dismissed", "successful", "failed"):
            return job_info

        # otherwise, query the ADES backend for the current status
        ades_resp = self._ades.get_job(job_spec)
        # print(ades_resp)
        job_info["status"] = ades_resp["status"]

        # populate current metrics
        job_info["metrics"] = ades_resp["metrics"]

        # and update the db with that status
        sqlite_update_job_status(job_id, job_info["status"], job_info["metrics"])
        return job_info

    def exec_job(self, proc_id, job_inputs):
        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")
        # TODO: this needs to be globally unique despite underlying processing cluster
        job_id = f"{proc_id}-{hashlib.sha1((json.dumps(job_inputs, sort_keys=True) + now).encode()).hexdigest()}"
        job_spec = {
            "process": self.get_proc(proc_id),
            "inputs": job_inputs,
            "job_id": job_id,
        }
        ades_resp = self._ades.exec_job(job_spec)
        # ades_resp will return platform specific information that should be
        # kept in the database with the job ID record
        sqlite_exec_job(proc_id, job_id, job_inputs, ades_resp)
        return {"jobID": job_id, "status": ades_resp["status"]}

    def dismiss_job(self, proc_id, job_id):
        job_spec = sqlite_dismiss_job(job_id)
        if job_spec:
            ades_resp = self._ades.dismiss_job(job_spec)
        return job_spec

    def get_job_results(self, proc_id, job_id):
        job_spec = self.get_job(proc_id, job_id)
        ades_resp = self._ades.get_job_results(job_spec)
        job_info = {
            "jobID": job_id,
            "status": ades_resp["status"],
            "links": ades_resp["links"],
        }
        return job_info
