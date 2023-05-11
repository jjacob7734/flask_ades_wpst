# Description

Flask app that implements a subset of the OGC ADES/WPST specification.	
Includes platform-specific implementations for Kubernetes and High End	
Computing (HEC) environments.

# Installation

Clone the repo and run the following in the top directory of the repo:

    python setup.py install

# Configuration

The following environment variables can be used to configure the application.

## General settings

These settings apply to all platforms.

| Variable Name | Description |
| ------------- | ----------- |
| `ADES_HOME` | Top level base directory for all files written by the app. |
| `ADES_PLATFORM` | Platform being run.  Can be set to `PBS` or `K8s` to activate the PBS (on high end computing systems like NASA Pleiades) or Kubernetes implementations, respectively.  Defaults to `Generic`, which does no extra platform specific actions. |

## PBS (High End Computing) Configuration

These settings apply to the PBS (High End Computing) platform only.

| Variable Name | Description |
| ------------- | ----------- |
| `ADES_PBS_QUEUE` | Queue name to use to submit jobs |
| `ADES_PBS_QUEUE_CACHE` | Queue name to use for handling of PBS preprocessing directives like data caching. Defaults to same queue as in `ADES_PBS_QUEUE` setting. |
| `ADES_DEFAULT_BUCKET_STAGEOUT` | Default bucket to use for stage-out.  | 

# Usage
Run the Flask app server with:

    python -m flask_ades_wpst.flask_wpst <options>

## Optional Arguments

    -h, --help            show this help message and exit
    -d, --debug           turn on Flask debug mode
    -H HOST, --host HOST  host IP address for Flask server
    -p PORT, --port PORT  host port number for Flask server
    -n NAME, --name NAME  ID of this ADES instance

## Running Multiple Instances

If you want to run multiple instances of this application, be sure to give
each instance a unique name/ID and port number to listen on.

# Try out the OGC ADES/WPS-T endpoints
You can see the available endpoints by starting with the root landing page
and inspecting the links returned:

## `getLandingPage`

Get a landing page showing the available endpoints.

    curl http://127.0.0.1:5000/

## `getProcesses`

Get a list of deployed processes.

    curl http://127.0.0.1:5000/processes

## `deployProcess`

To deploy a process use an HTTP `POST` with a `proc` parameter specifying the
URL of the application descriptor JSON file.  If this file is hosted on
Github, but sure to get the raw URL by clicking on the Raw button on the
page.

    curl -X POST http://127.0.0.1:5000/processes?proc=https://raw.githubusercontent.com/path/to/application-descriptor.json

## `getProcess`

Get information about a deployed process.  First get the process ID by
either inspecting the JSON returned from either `deployProcess` or
`getProcesses`.  Use that process ID in the URL as follows:

    curl http://127.0.0.1:5000/processes/<process-id>

## `undeployProcess`

Undeploy a process.  First get the process ID by inspecting the JSON returned
from either `deployProcess` or `getProcesses`.  Use that process ID in an HTTP
`DELETE` URL as follows:

    curl -X DELETE http://127.0.0.1:5000/processes/<process-id>
   
## `getJobList`

Get a list of jobs for a process (job type).

    curl http://127.0.0.1:5000/processes/<process-id>/jobs

## `executeJob`

To run a job, use an HTTP `POST` with the input values for the job specified in
the payload dictionary as indicated below.

    curl -H "Content-Type: application/json" -X POST -d '{<input-values-dictionary>}' http://127.0.0.1:5000/processes/<process-id>/jobs

## `getJobStatus`

Get status of a submitted job. First get the job ID by inspecting the JSON
returned from either `executeJob` or `getJobList`.   Use the job ID in the URL
as follows:

    curl http://127.0.0.1:5000/processes/<process-id>/jobs/<job-id>

## `dismissJob`

Dismiss a submitted job. First get the job ID by inspecting the JSON returned
from either `executeJob` or `getJobList`.   Use the job ID in and HTTP `DELETE`
URL as follows:

    curl -X DELETE http://127.0.0.1:5000/processes/<process-id>/jobs/<job-id>

Only jobs that have not yet completed can be dismissed.

## `getJobResults`

Once a job completes successfully (as indicted by `getJobStatus`), you can get
the URLs of the job results as follows:

    curl http://127.0.0.1:5000/processes/<process-id>/jobs/<job-id>/result

# Notes
1. This is a partial implementation of the OGC ADES/WPS-T specification:
http://docs.opengeospatial.org/per/18-050r1.html#_wps_t_restjson
1. The `deployProcess` `POST` requires a `proc` keyword with value
equal to the application descriptor JSON URL. If stored on Github, you must 
specify the raw URL, which you can get by navigating to the file and 
clicking on "Raw".   The process ID must be specified in that application
descriptor JSON.
1. The `executeJob` `POST` requires a dictionary payload with the required
job input values.
