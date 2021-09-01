from flask_ades_wpst.ades_abc import ADES_ABC


class ADES_PBS(ADES_ABC):

    def deploy_proc(self, proc_spec):
        return proc_spec

    def undeploy_proc(self, proc_spec):
        return proc_spec

    def exec_job(self, job_spec):
        return job_spec

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
