import argparse
import os
import re
import json
from datetime import datetime, timezone
from socket import getfqdn, gethostbyname
from shutil import disk_usage
from psutil import virtual_memory

def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-l", "--log", required=True,
                        help="input log file")
    parser.add_argument("-e", "--exitcode", required=True,
                        help="input exit_code.json file")
    parser.add_argument("-m", "--metrics", required=True,
                        help="output metrics.json file")
    args = parser.parse_args()
    return args.log, args.exitcode, args.metrics

def get_disk_gb(start_dir, excludes=[]):
    disk_bytes = 0
    for path, dirs, files in os.walk(start_dir):
        for f in files:
            fpath = os.path.join(path, f)
            if not any([os.path.relpath(fpath, start=start_dir).startswith(ex)
                        for ex in excludes]):
                cur_bytes = os.path.getsize(fpath)
                disk_bytes += os.path.getsize(fpath)
    return disk_bytes / 1073741824 # convert bytes to GB

def step_disk_usage(step_name):
    if step_name.startswith("stage-in"):
        # all storage for stage in step is assumed to be in the inputs
        # subdirectory.
        disk_gb = get_disk_gb("inputs")
    elif step_name.startswith("stage-out"):
        # stage out step is assumed to use no storage.
        disk_gb = 0
    else:
        # process step:  Add up size of the entire work directory, but 
        # excluding everything under the inputs subdirectory.
        disk_gb = get_disk_gb(".", excludes=["inputs"])
    return disk_gb

def reformat_dt(dt_str_src, dt_fmt_src, dt_fmt_dst):
    dt_src = datetime.strptime(dt_str_src, dt_fmt_src)
    dt_src_utc = dt_src.astimezone(tz=timezone.utc)
    dt_str_dst = dt_src_utc.strftime(dt_fmt_dst)
    return dt_str_dst

def get_step_info_from_log(log_fname):
    dt_fmt_src = "%Y-%m-%d %H:%M:%S"
    dt_fmt_dst = "%Y-%m-%dT%H:%M:%S%z"
    pattern_start = re.compile("\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\].+\[step\s(.+)\]\sstart")
    pattern_end = re.compile("\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\].+\[step\s(.+)\]\scompleted\s+(\w+)")
    
    with open(log_fname, 'r') as f:
        step_starts = []
        step_ends = []
        step_names = []
        step_status = []
        for line in f:
            match_start = pattern_start.search(line)
            if match_start is not None:
                # START of job step
                start_time = reformat_dt(match_start.group(1),
                                         dt_fmt_src, dt_fmt_dst)
                step_starts.append(start_time)
                step_names.append(match_start.group(2))
            else:
                match_end = pattern_end.search(line)
                if match_end is not None:
                    # END of job step
                    end_time = reformat_dt(match_end.group(1),
                                           dt_fmt_src, dt_fmt_dst)
                    step_ends.append(end_time)
                    step_status.append(match_end.group(3))
    return list(zip(step_names, step_starts, step_ends, step_status))

def duration_secs(start_time_str, end_time_str):
    dt_fmt = "%Y-%m-%dT%H:%M:%S%z"
    start_time = datetime.strptime(start_time_str, dt_fmt)
    end_time = datetime.strptime(end_time_str, dt_fmt)
    return end_time.timestamp() - start_time.timestamp()

def get_node_info():
    hostname = getfqdn()
    ip_addr = gethostbyname(hostname)
    memory_avail_bytes = virtual_memory().total
    disk_space_free_bytes = disk_usage('.').free
    bytes_per_gb = 1073741824
    return { "node_type": "broadwell",
             "cores": 1,
             "memory_gb": memory_avail_bytes / bytes_per_gb,
             "hostname": hostname,
             "ip_address": ip_addr,
             "disk_space_free_gb": disk_space_free_bytes / bytes_per_gb }

def get_workflow_metrics():
    metrics_steps = { workflow_step[0]: workflow_step[1]
                      for workflow_step in workflow_info }

def get_exit_code(exit_code_fname):
    # Load exit_code from JSON file.
    with open(exit_code_fname, 'r') as f:
        d = json.loads(f.read())
        exit_code = d["exit_code"]
    return exit_code

def get_pbs_metrics():
    # Parse input arguments
    log_fname, exit_code_fname, metrics_fname = parse_args()

    # Get information about the compute node used for the job execution.
    # For ADES_PBS, all the workflow steps run on the same node, so this 
    # information applies to the entire workflow.
    node_info = get_node_info()

    # Get exit code for the job, indicating if the job succeeded or failed.
    # For ADES_PBS, for now we are not capturing the exit code for each
    # step, so we assign each step the exit code of the overall workflow.
    exit_code = get_exit_code(exit_code_fname)

    # Get a list of tuples giving step name, start time (UTC), and end time 
    # (UTC) for each step of the job's workflow.
    job_steps = get_step_info_from_log(log_fname)

    # Construct metrics dictionary.
    metrics = { job_step[0] : 
                { "time_start" : job_step[1],
                  "time_end" : job_step[2],
                  "time_duration_seconds" : duration_secs(job_step[1],
                                                          job_step[2]),
                  "work_dir_size_gb": step_disk_usage(job_step[1]),
                  "memory_max_gb": -999.,
                  "exit_code": int(job_step[3] != "success"),
                  "node": node_info }
                for job_step in job_steps }

    # Add in workflow level metrics for the entire multi-step job.
    workflow_start = job_steps[0][1] # 1st start time
    workflow_end =  job_steps[-1][2] # last end time
    workflow_dir_size_gb = sum([metrics[d]["work_dir_size_gb"]
                                for d in metrics])
    workflow_metrics = { "time_start": workflow_start,
                         "time_end": workflow_end,
                         "time_duration_seconds": duration_secs(workflow_start,
                                                                workflow_end),
                         "work_dir_size_gb": workflow_dir_size_gb,
                         "memory_max_gb": -999.,
                         "exit_code": exit_code,
                         "node": node_info }
    metrics |= { "workflow": workflow_metrics }

    # Save metrics to work directory.
    with open(metrics_fname, 'w') as f:
        f.write(json.dumps(metrics, indent=4))
    print("Metrics written to {}".format(metrics_fname))


if __name__ == "__main__":
    get_pbs_metrics()
