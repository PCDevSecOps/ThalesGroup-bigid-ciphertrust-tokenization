#!/bin/bash
app="thales_bigid.api"
host_docker_ports=$(sed -nr "/^\[DockerDeploy\]/ { :l /^host_port[ ]*=/ { s/[^=]*=[ ]*//; p; q;}; n; b l;}" ./config.ini | tr -d '\r')
host_docker_ports+=":$(sed -nr "/^\[DockerDeploy\]/ { :l /^docker_link_port[ ]*=/ { s/[^=]*=[ ]*//; p; q;}; n; b l;}" ./config.ini | tr -d '\r')"
cts_ip=$(sed -nr "/^\[CTS\]/ { :l /^ip[ ]*=/ { s/[^=]*=[ ]*//; p; q;}; n; b l;}" ./config.ini | tr -d '\r')
cts_hostname=$(sed -nr "/^\[CTS\]/ { :l /^hostname[ ]*=/ { s/[^=]*=[ ]*//; p; q;}; n; b l;}" ./config.ini | tr -d '\r')

docker build -t ${app} .
docker run -d -p ${host_docker_ports} \
  --name=${app} \
  --add-host ${cts_hostname}:${cts_ip} \
  -v $PWD:/app ${app}
