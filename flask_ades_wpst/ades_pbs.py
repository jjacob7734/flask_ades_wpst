import os
import shutil
from subprocess import run
import json
from flask_ades_wpst.ades_abc import ADES_ABC
from pprint import pprint


class ADES_PBS(ADES_ABC):

    def __init__(self, ades_id, 
                 base_ades_home_dir='./ades', base_work_dir='./jobs',
                 job_inputs_fname='inputs.json',
                 sing_stash_dir='./singularity', module_cmd='modulecmd',
                 singularity_cmd='./bin/singularity',
                 pbs_qsub_cmd='./bin/qsub', pbs_qdel_cmd='./bin/qdel',
                 pbs_qstat_cmd='./bin/qstat', pbs_script_fname='pbs.bash',
                 exit_code_fname="exit_code.json",
                 cwl_runner_log_fname="cwl_runner.log",
                 metrics_fname="metrics.json", pbs_script_stub="""#!/bin/bash
#
#PBS -q normal
#PBS -lselect=1:ncpus=1:model=bro
#PBS -lwalltime=8:00:00
#PBS -lsite=static_broadwell:nat=hfe1
#
# Setup
module load singularity
. $HOME/.venv/ades/bin/activate
cd {}
#
# Run workflow
cwl-runner --singularity --no-match-user --no-read-only --tmpdir-prefix {} --leave-tmpdir --timestamps {} {} > {} 2>&1
echo {{\\"exit_code\\": $?}} > {}
python -m flask_ades_wpst.get_pbs_metrics -l {} -m {} -e {}
"""):
        self._ades_id = ades_id
        self._ades_home_dir = os.path.join(base_ades_home_dir,
                                           self._ades_id)
        if not os.path.isdir(self._ades_home_dir):
            os.mkdir(self._ades_home_dir)
        self._base_work_dir = os.path.join(self._ades_home_dir,
                                           base_work_dir)
        if not os.path.isdir(self._base_work_dir):
            os.mkdir(self._base_work_dir)
        self._job_inputs_fname = job_inputs_fname
        self._sing_stash_dir = os.path.join(self._ades_home_dir,
                                            sing_stash_dir)
        if not os.path.isdir(self._sing_stash_dir):
            os.mkdir(self._sing_stash_dir)
        self._pbs_script_fname = pbs_script_fname
        self._module_cmd = module_cmd
        self._singularity_cmd = singularity_cmd
        self._pbs_qsub_cmd = pbs_qsub_cmd
        self._pbs_qdel_cmd = pbs_qdel_cmd
        self._pbs_qstat_cmd = pbs_qstat_cmd
        self._exit_code_fname = exit_code_fname
        self._cwl_runner_log_fname = cwl_runner_log_fname
        self._metrics_fname = metrics_fname
        self._pbs_script_stub = pbs_script_stub

    def _construct_sif_name(self, docker_url):
        sif_name = os.path.basename(docker_url).replace(':', '_') + ".sif"
        return os.path.join(self._sing_stash_dir, sif_name)

    def _construct_workdir(self, job_id):
        return os.path.abspath(os.path.join(self._base_work_dir, job_id))

    def _construct_pbs_job_id_from_qsub_stdout(self, qsub_stdout):
        return '.'.join(qsub_stdout.strip().split('.')[:2])
        
    def _validate_workdir(self, work_dir):
        if (os.path.isdir(work_dir) and
            os.path.isfile(os.path.join(work_dir, self._pbs_script_fname))):
            work_dir_abspath = os.path.abspath(work_dir)
            base_work_dir_abspath = os.path.abspath(self._base_work_dir)
            return (len(work_dir_abspath) > len(base_work_dir_abspath) and 
                    work_dir_abspath.startswith(base_work_dir_abspath))
        else:
            return False

    def _remove_workdir(self, job_id):
        work_dir = self._construct_workdir(job_id)
        if self._validate_workdir(work_dir):
            shutil.rmtree(work_dir)

    def _pbs_job_state_to_status_str(self, work_dir, job_state):
        pbs_job_state_to_status = {
            "Q": "accepted",
            "R": "running",
            "E": "running",
        }
        if job_state in pbs_job_state_to_status:
            status =  pbs_job_state_to_status[job_state]
        elif job_state == "F":
            # Job finished; need to check cwl-runner exit-code to determine
            # if the job succeeded or failed.  In the auto-generated, PBS job 
            # submission script, the exit code is saved to a file.
            exit_code_fname = os.path.join(work_dir, self._exit_code_fname)
            try:
                with open(exit_code_fname, "r") as f:
                    d = json.loads(f.read())
                    exit_code = d["exit_code"]
                    if exit_code == 0:
                        status = "successful"
                    else:
                        status = "failed"
            except:
                status = "unknown-not-qref"
        else:
            # Encountered a PBS job state that is not supported.
            status = "unknown-no-exit-code"
        return status

    def _get_status_from_qstat_stdout(self, work_dir, qstat_stdout):
        # Get PBS job state from qstat json-formatted stdout.
        qstat_json = json.loads(qstat_stdout)
        try:
            job_state = list(qstat_json["Jobs"].values())[0]["job_state"]
        except:
            status = "unknown-no-job-state"
        else:
            # Convert PBS job state to ADES status string
            status = self._pbs_job_state_to_status_str(work_dir, job_state)
        return status

    def deploy_proc(self, proc_spec):
        container = proc_spec["executionUnit"][0]["href"]
        local_sif = self._construct_sif_name(container)
        print("Localizing container {} to {}".format(container, local_sif))
        run([self._module_cmd, "bash", "load", "singularity"])
        run([self._singularity_cmd, "pull", local_sif, container])
        return proc_spec

    def undeploy_proc(self, proc_spec):
        container = proc_spec["executionUnit"]
        local_sif = self._construct_sif_name(container)
        if os.path.exists(local_sif):
            print("Removing local SIF {}".format(local_sif))
            os.remove(local_sif)
        return proc_spec

    def exec_job(self, job_spec):
        print("Executing:", job_spec)

        # Create working directory for the job with the same name as the
        # job identifier.
        job_id = job_spec["job_id"]
        work_dir = self._construct_workdir(job_id)
        try:
            os.mkdir(work_dir)
        except:
            raise OSError("Could not create work directory {} for job {}".format(work_dir, job_id))

        # Write job inputs to a JSON file in the work directory.
        job_inputs_fname = os.path.join(work_dir, self._job_inputs_fname)
        with open(job_inputs_fname, 'w', encoding='utf-8') as job_inputs_file:
            json.dump(job_spec['inputs'], job_inputs_file, ensure_ascii=False,
                      indent=4)

        # Create PBS script in the work directory.
        pbs_script_fname = os.path.join(work_dir, self._pbs_script_fname)
        with open(pbs_script_fname, 'w') as pbs_script_file:
            # The second format string below is the cwl-runner's tmpdir-prefix,
            # which we set to the same as the work directory.  The os.path.join
            # with '' is a trick to ensure that the trailing slash is included
            # in the path.
            pbs_script_file.write(self._pbs_script_stub.\
                                  format(work_dir, 
                                         os.path.join(work_dir, ''),
                                         job_spec['process']['owsContextURL'],
                                         job_inputs_fname,
                                         self._cwl_runner_log_fname,
                                         self._exit_code_fname,
                                         self._cwl_runner_log_fname,
                                         self._metrics_fname,
                                         self._exit_code_fname))

        # Submit job to queue for execution.
        qsub_resp = run([self._pbs_qsub_cmd, "-N", job_id, "-o", work_dir, 
                         "-e", work_dir, pbs_script_fname], 
                        capture_output=True, text=True)
        print("qsub_resp:", qsub_resp)
        error = qsub_resp.stderr
        if qsub_resp.returncode == 0:
            pbs_job_id = \
                self._construct_pbs_job_id_from_qsub_stdout(qsub_resp.stdout)
            status = 'accepted'
        else:
            pbs_job_id = 'none'
            status = 'failed'
            
        return {'pbs_job_id': pbs_job_id, 'status': status, 'error': error}

    def dismiss_job(self, job_spec):
        # We can only dismiss jobs that were last in accepted or running state.
        status = self.get_job(job_spec)["status"]
        if status in ("running", "accepted"):
            # Delete the job from the queue if it is still queued or running.
            # The "-x" option enables deleting jobs and their history in any 
            # of the following states: running, queued, suspended, held, 
            # finished, or moved.
            pbs_job_id = job_spec["backend_info"]["pbs_job_id"]
            qdel_resp = run([self._pbs_qdel_cmd, "-x", "-W", "force", pbs_job_id],
                            capture_output=True, text=True)
            print("Deleting jobID:", job_spec["jobID"])
            print("Deleting pbs_job_id:", pbs_job_id)
            print("qdel_resp:", qdel_resp)
       
        # Remove the job's work directory.
        job_id = job_spec["jobID"]
        self._remove_workdir(job_id)
            
        return job_spec

    def get_job(self, job_spec):
        # Get PBS job status.
        # 
        job_id = job_spec["jobID"]
        work_dir = self._construct_workdir(job_id)
        pbs_job_id = job_spec["backend_info"]["pbs_job_id"]
        qstat_resp = run([self._pbs_qstat_cmd, "-x", "-F", "json", pbs_job_id],
                         capture_output=True, text=True)
        print("qstat_resp:", qstat_resp)
        job_spec["status"] = \
            self._get_status_from_qstat_stdout(work_dir, qstat_resp.stdout)

        metrics_fname = os.path.join(work_dir, self._metrics_fname)
        if os.path.exists(metrics_fname):
            # Read metrics from file create after execution.
            with open(metrics_fname, 'r') as f:
                job_spec["metrics"] = json.loads(f.read())
        else:
            # Initialize metrics to empty dict to be populated after execution.
            job_spec["metrics"] = {}
        
        return job_spec

    def get_job_results(self, job_spec):
        res =  {"links": [{"href": "https://mypath",
                           "rel": "result",
                           "type": "application/json",
                           "title": "mytitle"}]}
        return {**job_spec, **res}
