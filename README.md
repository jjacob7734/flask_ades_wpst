# Description
Stub flask app that implements a subset of the OGC ADES/WPST specification.

# Get started
Clone the repo and create a subdirectory for the SQLite database file.

    git clone https://github.jpl.nasa.gov/SOAMC/flask_ades_wpst.git
    cd flask_ades_wpst
    mkdir sqlite

# Install it natively (not in a container) as a python module
Be sure to follow the steps in the "Get started" section above first.  Install
natively as a python module with:

    python setup.py install

The `Flask` python module is required for installation.

# Run it natively (not in a container)
Be sure to follow the steps in the "Get started" section above first.
Run the Flask app server with:

    python -m flask_ades_wpst.flask_wpst

# Run it as a Docker container
Be sure to follow the steps in the "Get started" section above first.
To run as a Docker container, but sure to do the following in the `docker run`
command as shown in the example below:

1. Map the Flask application server port to the host port (`-p` option)
1. Mount your `sqlite` subdirectory on the host machine in to the container
(`-v` option)
1. Set the `ADES_PLATFORM` environment variable to a supported environment
(e.g., `Argo`, `PBS`, `Generic`) (`-v` option).  If no environment variable
is set, the default is `Generic`, which results in no additional actions
being done on the host.

Run with Docker: 

In the following, set the `ADES_PLATFORM` environment variable to the
appropriate setting for your platform (examples: Argo, PBS)

    docker run -it -p 5000:5000 -v ${PWD}/sqlite:/flask_ades_wpst/sqlite -e "ADES_PLATFORM=<platform>" <org>/flask-ades-wpst:<tag>

# Build the container locally
Be sure to follow the steps in the "Get started" section above first.
If you run the Docker container as shown above, you will automatically download
the latest container version from Docker Hub.  If you like, you can also build
your own local container as follows:

    docker build -t <org>/flask-ades-wpst:<tag> -f docker/Dockerfile .
    
# Try out the OGC ADES/WPS-T endpoints
You can see the available endpoints by starting with the root endpoint and inspecting the links returned:

    curl http://127.0.0.1:5000/

# Notes
1. This is a partial implementation of the OGC ADES/WPS-T specification:
http://docs.opengeospatial.org/per/18-050r1.html#_wps_t_restjson
1. The `deployProcess` `POST` requires a `proc` keyword with value
equal to the application descriptor JSON URL. The process ID must be
specified in that application descriptor JSON.
1. The `executeJob` `POST` requires a `job` keyword with value equal
to the job CWL URL.
1. You can get the above URLs by navigating to the file on github.com
and clicking on "Raw".
