def get_procs():
    procs = [{"processSummary": {"id":"proc1",
                                 "title": "proc1 title",
                                 "abstract": "proc1_abstract",
                                 "keywords": "proc1_keywords"}},
             {"processSummary": {"id":"proc2",
                                 "title": "proc2 title",
                                 "abstract": "proc2_abstract",
                                 "keywords": "proc2_keywords"}}]
    return procs

def get_proc(proc_id):
    proc_info = {"id": proc_id, "title": "my process title",
                 "inputs": [], "outputs": [],
                 "executionEndpoint": "https://myhost/endpoint"}
    return proc_info

def deploy_proc(proc_spec):
    return proc_spec
            
def undeploy_proc(proc_id):
    proc_info = {"id": proc_id}
    return proc_info

def get_jobs():
    jobs = ["job1", "job2", "job3"]
    return jobs

def get_job(proc_id, job_id):
    # Required fields:
    #   jobID (str)
    #   status (str) in ["accepted" | "running" | "succeeded" | "failed"]
    # Optional fields:
    #   expirationDate (dateTime)
    #   estimatedCompletion (dateTime)
    #   nextPoll (dateTime)
    #   percentCompleted (int) in range [0, 100]
    job_info = {"jobID": job_id, "status": "running"}
    return job_info

def exec_job(inputs):
    job_id = "job5"
    status_url = "https://myhost/status/{}".format(job_id)
    return status_url
            
def dismiss_job(proc_id, job_id):
    job_status = "canceled" # what string to use here?
    dismiss_status = {"jobID": job_id, "status": job_status}
    return dismiss_status

def get_job_results(proc_id, job_id):
    job_results = ["file:///path/to/result1",
                   "file:///path/to/result2"]
    return job_results
