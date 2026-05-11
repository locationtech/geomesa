#!/usr/bin/env bash
#
# Build + publish the spatial_iceberg connector carrier image to ECR.
#
# The image carries only the connector fat JAR (no Trino runtime); a Kubernetes init container
# copies it onto the Trino plugin path. See apps/query-api/values/trino.yaml.gotmpl in the
# thresher-helmfiles repo.
#
# All build/push logic + config (REGISTRY/REGION/AWS_*/TAG/USE_PUBLISHED_JAR/...) lives in
# docker/_deploy-lib.sh. Quick start:
#   AWS_REGION=… ./deploy.sh                                                   # account auto-derived; tag dev-latest
#   REGISTRY=<acct>.dkr.ecr.<region>.amazonaws.com TAG=2026.06.03 ./deploy.sh  # explicit override
set -euo pipefail

MODULE="geomesa-trino-plugin"
source "$(dirname "${BASH_SOURCE[0]}")/../_deploy-lib.sh"
