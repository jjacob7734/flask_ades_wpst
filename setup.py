from setuptools import setup, find_packages

setup(
    name="flask_ades_wpst",
    version="0.1",
    packages=find_packages(),
    author="",
    author_email="",
    description="Base Flask app implementing OGC/ADES WPS-T specification",
    long_description=open('README.md').read(),
    keywords="ADES WPS-T Flask SOAMC HySDS JPL",
    url="https://github.jpl.nasa.gov/SOAMC/flask_ades_wpst.git",
    project_urls={
        "Source Code": "https://github.jpl.nasa.gov/SOAMC/flask_ades_wpst.git",
        "Container": "https://hub.docker.com/r/jjacob7734/flask-ades-wpst"
    },
    install_requires=[
        "Flask==1.1.2",
        "requests==2.24.0",
        "pyyaml==5.4.1",
        "kubernetes==19.15.0"
    ]
)
