# Description
Stub flask app that implements a subset of the OGC ADES/WPST specification.

# Get started
Clone the repo and create a subdirectory for the SQLite database file.

    git clone https://github.jpl.nasa.gov/SOAMC/flask_ades_wpst.git
    cd flask_ades_wpst
    mkdir sqlite

# Install it natively (not in a container) as a python module

    python setup.py install

# Run it natively (not in a container)
Run with:

    python -m flask_ades_wpst.flask_wpst

# Run it as a Docker container
To run as a Docker container, but sure to map the Flask application server
port to the host port (`-p` option) and mount your `sqlite` subdirectory on the
host machine in to the container (`-v` option) as shown below.

    docker run -it -p 5000:5000 -v ${PWD}/sqlite:/flask_ades_wpst/sqlite jjacob7734/flask-ades-wpst:latest

# Build the container locally

    docker build -t flask-ades-wpst:latest -f docker/Dockerfile .
    
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
