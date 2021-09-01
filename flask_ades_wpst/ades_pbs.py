from flask_ades_wpst.ades_abc import ADES_ABC


class ADES_PBS(ADES_ABC):

    def deploy_proc(self, proc_spec):
        return {}

    def undeploy_proc(self, proc_spec):
        return {}

    def exec_job(self, job_spec):
        return {}

    def dismiss_job(self, job_spec):
        return {}

    def get_job(self, job_spec):
        return {"status": "none"}

    def get_job_results(self, job_spec):
        return {"status": "none",
                "links": [{"href": "https://mypath",
                           "rel": "result",
                           "type": "application/json",
                           "title": "mytitle"}]}
