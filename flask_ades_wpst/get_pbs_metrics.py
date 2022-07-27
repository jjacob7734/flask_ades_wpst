import argparse
import os
import re
from datetime import datetime, timezone
from pprint import pprint
import socket
import shutil
import json
import psutil

def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-l", "--log", required=True,
                        help="input log file")
    parser.add_argument("-e", "--exitcode", required=True,
                        help="input exit_code.json file")
    parser.add_argument("-p", "--pbs", required=True,
                        help="input pbs.bash script")
    parser.add_argument("-m", "--metrics", required=True,
                        help="output metrics.json file")
    args = parser.parse_args()
    return args.log, args.exitcode, args.pbs, args.metrics

def get_disk_mb(start_dir, excludes=[]):
    disk_bytes = 0
    for path, dirs, files in os.walk(start_dir):
        for f in files:
            fpath = os.path.join(path, f)
            if not any([os.path.relpath(fpath, start=start_dir).startswith(ex)
                        for ex in excludes]):
                cur_bytes = os.path.getsize(fpath)
                disk_bytes += os.path.getsize(fpath)
    return disk_bytes / 1048576 # convert bytes to MB

def step_disk_usage(step_name):
    if step_name == "stage_in":
        # all storage for stage in step is assumed to be in the inputs
        # subdirectory.
        disk_mb = get_disk_mb("inputs")
    elif step_name == "stage_out":
        # stage out step is assumed to use no storage.
        disk_mb = 0
    else:
        # process step:  Add up size of the entire work directory, but 
        # excluding everything under the inputs subdirectory.
        disk_mb = get_disk_mb(".", excludes=["inputs"])
    return disk_mb

def reformat_dt(dt_str_src, dt_fmt_src, dt_fmt_dst):
    dt_src = datetime.strptime(dt_str_src, dt_fmt_src)
    dt_src_utc = dt_src.astimezone(tz=timezone.utc)
    dt_str_dst = dt_src_utc.strftime(dt_fmt_dst)
    return dt_str_dst

def get_step_times_from_log(log_fname):
    dt_fmt_src = "%Y-%m-%d %H:%M:%S"
    dt_fmt_dst = "%Y-%m-%dT%H:%M:%S%z"
    pattern_start = re.compile("\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\].+\[step\s(.+)\]\sstart")
    pattern_end = re.compile("\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\].+\[step\s(.+)\]\scompleted")
    
    with open(log_fname, 'r') as f:
        step_starts = []
        step_ends = []
        step_names = []
        for line in f:
            match_start = pattern_start.search(line)
            if match_start is not None:
                print("START", line)
                start_time = reformat_dt(match_start.group(1),
                                         dt_fmt_src, dt_fmt_dst)
                step_starts.append(start_time)
                step_names.append(match_start.group(2))
            else:
                match_end = pattern_end.search(line)
                if match_end is not None:
                    print("END", line)
                    end_time = reformat_dt(match_end.group(1),
                                           dt_fmt_src, dt_fmt_dst)
                    step_ends.append(end_time)
    return list(zip(step_names, step_starts, step_ends))

def step_duration_seconds(start_time_str, end_time_str):
    dt_fmt = "%Y-%m-%dT%H:%M:%S%z"
    start_time = datetime.strptime(start_time_str, dt_fmt)
    end_time = datetime.strptime(end_time_str, dt_fmt)
    return end_time.timestamp() - start_time.timestamp()

def get_usage_stats(log_fname):
    step_times = get_step_times_from_log(log_fname)
    print(step_times)
    usage_stats = {"children": 
                   [{"name": step_time[0],
                     "start_time": step_time[1],
                     "finish_time": step_time[2],
                     "cpus": 1.0,
                     "ram_megabytes": -999.,
                     "ram_megabytes_hours": -999.,
                     "disk_megabytes": step_disk_usage(step_time[0])} 
                    for step_time in step_times]}
    for step_usage_stats in usage_stats["children"]:
        step_usage_stats["elapsed_seconds"] = \
            step_duration_seconds(step_usage_stats["start_time"], 
                                  step_usage_stats["finish_time"])
        step_usage_stats["elapsed_hours"] = \
            step_usage_stats["elapsed_seconds"] / 3600
        step_usage_stats["cpu_hours"] = \
            step_usage_stats["elapsed_hours"] * step_usage_stats["cpus"]
    usage_stats |= {"cores_allowed": 1.,
                    "finish_time": usage_stats["children"][-1]["finish_time"],
                    "max_parallel_cpus": 1.,
                    "max_parallel_ram_megabytes": -999.,
                    "max_parallel_tasks": 1,
                    "ram_mb_allowed": -999.,
                    "start_time": usage_stats["children"][0]["start_time"],
                    "total_cpu_hours": \
                    sum([ch["cpu_hours"] for ch in usage_stats["children"]]),
                    "total_disk_megabytes": \
                    sum([ch["disk_megabytes"]
                         for ch in usage_stats["children"]]),
                    "total_ram_megabyte_hours": -999.,
                    "total_tasks": len(usage_stats["children"])}
    usage_stats |= {"elapsed_seconds": \
                    step_duration_seconds(usage_stats["start_time"],
                                          usage_stats["finish_time"])}
    usage_stats |= {"elapsed_hours": usage_stats["elapsed_seconds"] / 3600}
    return usage_stats

def get_process_stats(usage_stats):
    hostname = socket.getfqdn()
    ip_addr = socket.gethostbyname(hostname)
    disk_space_free_bytes = shutil.disk_usage('.').free
    process_stats = [{
        "name": child["name"],
        "time_started": child["start_time"],
        "time_end": child["finish_time"],
        "work_dir_size_gb": child["disk_megabytes"] / 1024,
        "memory_max_gb": -999.,
        "node": {
            "cores": child["cpus"],
            "memory_gb": psutil.virtual_memory().total / 1073741824, # GB
            "hostname": hostname,
            "ip_address": ip_addr,
            "disk_space_free_gb": disk_space_free_bytes / 1073741824 # GB
        }} for child in usage_stats["children"]]
    return process_stats

def get_workflow_stats(usage_stats, exit_code_fname, pbs_bash_fname):
    # The PBS bash script creation time is a good approximation of the 
    # time the job was queued, because those events both happened in the 
    # execute job call.
    dt_fmt = "%Y-%m-%dT%H:%M:%S%z"
    queue_ts = os.path.getctime(pbs_bash_fname)
    print("timestamp", pbs_bash_fname, queue_ts)
    queue_dt = datetime.fromtimestamp(queue_ts).astimezone(tz=timezone.utc)
    queue_dt_str = queue_dt.strftime(dt_fmt)

    # Load exit_code from JSON file.
    with open(exit_code_fname, 'r') as f:
        d = json.loads(f.read())
        exit_code = d["exit_code"]
    
    # Populate workflow stats.
    workflow_stats = {"exit_code": exit_code,
                      "time_queued": queue_dt_str,
                      "time_started": usage_stats["children"][0]["start_time"],
                      "time_end": usage_stats["children"][0]["finish_time"] }
    return workflow_stats

def get_pbs_metrics():
    log_fname, exit_code_fname, pbs_bash_fname, metrics_fname = parse_args()
    usage_stats = get_usage_stats(log_fname)
    process_stats = get_process_stats(usage_stats)
    workflow_stats = get_workflow_stats(usage_stats, exit_code_fname,
                                        pbs_bash_fname)
    metrics = { "blob": usage_stats,
                "processes": process_stats,
                "workflow": workflow_stats }
    with open(metrics_fname, 'w') as f:
        f.write(json.dumps(metrics, indent=4))
    print("Metrics written to {}".format(metrics_fname))


if __name__ == "__main__":
    get_pbs_metrics()
