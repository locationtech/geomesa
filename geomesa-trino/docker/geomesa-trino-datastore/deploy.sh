#!/usr/bin/env bash
#
# Build + publish the GeoMesa Trino DataStore carrier image to ECR.
#
# The image carries the GeoTools DataStore JAR + the Trino JDBC driver (no runtime); a Kubernetes
# init container copies them onto the query-api worker / layer-manager classpath extension dir
# (/app/extensions). See workers.yaml.gotmpl / layers.yaml.gotmpl in the thresher-helmfiles repo.
#
# All build/push logic + config (REGISTRY/REGION/AWS_*/TAG/USE_PUBLISHED_JAR/...) lives in
# docker/_deploy-lib.sh. Quick start:
#   AWS_REGION=… ./deploy.sh                                                   # account auto-derived; tag dev-latest
#   REGISTRY=<acct>.dkr.ecr.<region>.amazonaws.com TAG=2026.06.04 ./deploy.sh  # explicit override
set -euo pipefail

MODULE="geomesa-trino-datastore"
# The datastore image also carries the Trino JDBC driver; staged too when USE_PUBLISHED_JAR=true.
TRINO_JDBC_VERSION="${TRINO_JDBC_VERSION:-481}"
EXTRA_ARTIFACTS=("io.trino:trino-jdbc:${TRINO_JDBC_VERSION}")
source "$(dirname "${BASH_SOURCE[0]}")/../_deploy-lib.sh"
