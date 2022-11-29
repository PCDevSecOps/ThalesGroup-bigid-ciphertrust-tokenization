FROM tiangolo/uwsgi-nginx-flask:python3.8

RUN apt update && apt install -y bash

COPY python-requirements.txt /tmp/python-requirements.txt
RUN pip install --no-cache-dir -r /tmp/python-requirements.txt
