import json
from flask_ades_wpst.ades_abc import ADES_ABC


class ADES_K8s(ADES_ABC):

    def deploy_proc(self, proc_spec):
        return proc_spec

    def undeploy_proc(self, proc_spec):
        return proc_spec

    def exec_job(self, job_spec):
        print(f"in ADES_K8s.exec_job(): job_spec={json.dumps(job_spec, indent=2)}")
        ades_resp = { "status": "accepted"}
        return ades_resp

    def dismiss_job(self, job_spec):
        return job_spec

    def get_job(self, job_spec):
        return job_spec

    def get_job_results(self, job_spec):
        res =  {"links": [{"href": "https://mypath",
                           "rel": "result",
                           "type": "application/json",
                           "title": "mytitle"}]}
        return {**job_spec, **res}
