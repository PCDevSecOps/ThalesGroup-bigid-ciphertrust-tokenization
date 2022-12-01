FROM tiangolo/uwsgi-nginx-flask:python3.8

RUN apt update && apt install -y bash

# ENV http_proxy http://<host><port>
# ENV https_proxy http://<host>:<port>

COPY python-requirements.txt /tmp/python-requirements.txt
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --no-cache-dir -r /tmp/python-requirements.txt
