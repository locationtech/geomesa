#!/usr/bin/env bash
#
# Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#
# Do not run this script manually. It is part of the automated bootstrap process and is not written for end user use.
#
# Core Bootstrap Script for GeoMesa on AWS EMR
#
# Note this script runs twice. It is initiated by AWS before the cluster is ready. During its lifetime it downloads a
# copy of the tools distribution and on the master spawns a clone of itself that waits for the cluster to finish
# setting up before continuing the bootstrap process.

logFile=/tmp/bootstrap.log
function log() {
  timeStamp=$(date +"%T")
  echo "${timeStamp}| ${@}" | tee -a $logFile
}

statusFile=/tmp/status.log
function updateStatus() {
  timeStamp=$(date +"%T")
  echo "${@}" > $statusFile
  sudo -H -u ec2-user aws s3 cp $statusFile ${CONTAINER}status.log
}

function failWithStatus() {
  msg="Fail: ${1}"
  updateStatus $msg
  exit 1
}

ARGS=($@) # Save the input args so we can pass them to the child.
VERSION=%%project.version%%
GM_TOOLS_DIST="%%gmtools.assembly.name%%"
GM_TOOLS_HOME="/opt/${GM_TOOLS_DIST}"
CHILD=false
CID=$(cat /mnt/var/lib/info/job-flow.json | jq .jobFlowId)

while [[ $# -gt 0 ]]; do
  case $1 in
    -z|--zeppelin)
      ZEPPELIN=true
      shift
      ;;
    -j*|--jupyter*)
      JUPYTER=true
      JUPYTER_PASSWORD="${1#*=}"
      if [[ -z "${JUPYTER_PASSWORD}" ]]; then
        JUPYTER_PASSWORD="geomesa"
      fi
      shift
      ;;
    -c=*|--container=*)
      CONTAINER="${1#*=}"
      shift
      ;;
    --child)
      CHILD=true
      shift
      ;;
    *)
      echo "[Warning] Unknown parameter: ${1}"
      shift
      ;;
  esac
done

log "Bootstrap Actions Spawned"

# Validate Parameters
if [[ -z "${CONTAINER}" ]]; then
  log "S3 container is required"
  failWithStatus "Failed to retrieve "
  exit 1
elif [[ "${CONTAINER}" != */ ]]; then
  # We need a trailing '/' for consistency
  CONTAINER="${CONTAINER}/"
fi

if [[ "${CHILD}" != "true" ]]; then
  log "Parent Process"

  # Parses a configuration file put in place by EMR to determine the role of this node
  isMaster=$(jq '.isMaster' /mnt/var/lib/info/instance.json)
  log "isMaster: ${isMaster}" 

  ### MAIN ####
  log "Main" 
  if [[ "${isMaster}" == "true" ]]; then
    updateStatus "Bootstrap Started"
    log "Copying Resources Locally"
    sudo aws s3 cp ${CONTAINER}${GM_TOOLS_DIST}-bin.tar.gz /opt/${GM_TOOLS_DIST}-bin.tar.gz
    [[ $? ]] || failWithStatus "Failed to copy GeoMesa tools distribution to Master"

    log "Setting Up Tools"
    pushd /opt/ \
      && sudo tar xf ${GM_TOOLS_DIST}-bin.tar.gz \
      && sudo chown -R ec2-user ${GM_TOOLS_HOME} \
      || failWithStatus "Failed to setup GeoMesa tools distribution"

    log "Starting Child Script" 
    sudo nohup ${GM_TOOLS_HOME}/bin/aws-utils/.aws-bootstrap-actions.sh --child ${ARGS[@]} &>/dev/null &
    [[ $? ]] || failWithStatus "Failed to start child setup script"

    log "Parent Done"
    exit 0

  else
    log "NO-OP"
    exit 0
  fi

else
  log "Child Process"

  # Wait until hbase is installed
  log "Waiting for HBase to be installed"
  updateStatus "Waiting for HBase installation"
  hbaseInstalled=$(ls /usr/bin/ | grep hbase)
  while [[ -z "${hbaseInstalled}" ]]; do
    sleep 3
    hbaseInstalled=$(ls /usr/bin/ | grep hbase)
    log "Sleeping" 
  done

  # Wait until hbase is running
  log "Setting up HBase status script" 
  echo "status" >> /tmp/status
  echo "exit" >> /tmp/status
  log "Waiting for HBase to start"
  updateStatus "Waiting for HBase to start"
  status=$(/usr/bin/hbase shell < /tmp/status | grep "active master")
  log "${status}" 
  while [[ -z "${status}" ]]; do
    log "Sleeping"
    sleep 1
    status=$(/usr/bin/hbase shell < /tmp/status | grep "active master")
  done

  log "Bootstrapping GeoMesa"
  updateStatus "Bootstrapping GeoMesa"
  sudo ${GM_TOOLS_HOME}/bin/aws-utils/aws-bootstrap-geomesa-hbase.sh \
    || failWithStatus "Failed to bootstrap GeoMesa"

  if [[ -n "${ZEPPELIN}" ]]; then
    updateStatus "Configuring Zeppelin"
    sudo ${GM_TOOLS_HOME}/bin/aws-utils/aws-bootstrap-geomesa-zeppelin.sh \
      || failWithStatus "Failed to bootstrap Zeppelin"
    log "Zeppelin Configured"
  fi

  if [[ -n "${JUPYTER}" ]]; then
    updateStatus "Installing Jupyter"
    sudo ${GM_TOOLS_HOME}/bin/aws-utils/aws-bootstrap-geomesa-jupyter.sh "${JUPYTER_PASSWORD}" \
      || failWithStatus "Failed to bootstrap Jupyter"
    log "Jupyter Installed"
  fi

  log "Bootstrap Complete"
  updateStatus "Done"
fi
