FROM ubuntu:20.04

RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev

WORKDIR /flask_ades_wpst

COPY . /flask_ades_wpst

RUN python3 setup.py install

ENTRYPOINT [ "python3" ]

CMD [ "-m", "flask_ades_wpst.flask_wpst", "--host=0.0.0.0" ]
