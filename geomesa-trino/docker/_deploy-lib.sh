#!/usr/bin/env bash
#
# Shared library for the geomesa-trino carrier-image deploy scripts. A wrapper
# (docker/<module>/deploy.sh) sets MODULE (+ optional EXTRA_ARTIFACTS), then sources this.
# It builds + pushes the <MODULE> carrier image to ECR (the image carries only the JAR(s);
# a Kubernetes init container copies them onto the Trino/worker classpath).
#
# Inputs (env vars; wrapper-set or caller-overridable):
#   MODULE             (required) module dir + artifactId, e.g. geomesa-trino-plugin
#   EXTRA_ARTIFACTS    (optional bash array) extra Maven coords "group:artifact:version" to also
#                      stage when USE_PUBLISHED_JAR=true (e.g. io.trino:trino-jdbc:481)
#   GROUP              Maven groupId of MODULE (default org.locationtech.geomesa)
#   REPO               ECR repository (default ccri/$MODULE)
#   TAG                image tag (default dev-latest)
#   REGISTRY           full ECR registry; if unset, derived as <account>.dkr.ecr.<region>...
#   REGION             ECR region; if unset, from AWS_REGION / AWS_DEFAULT_REGION / aws config
#   AWS_ACCOUNT_ID     ECR account; if unset (and no REGISTRY), from 'aws sts get-caller-identity'
#   USE_PUBLISHED_JAR  true => pull the published JAR(s) (needs JAR_VERSION) instead of local dist/
#   JAR_VERSION        version to pull when USE_PUBLISHED_JAR=true
#   MAVEN_REPO_LOCAL   local repo to copy fetched JARs from (default ~/.m2/repository)
set -euo pipefail

: "${MODULE:?_deploy-lib.sh: set MODULE (e.g. geomesa-trino-plugin) before sourcing}"
GROUP="${GROUP:-org.locationtech.geomesa}"

# Region + ECR registry come from AWS_* env vars / the active credentials (no hardcoded account).
# Override REGION or the full REGISTRY to bypass derivation.
REGION="${REGION:-${AWS_REGION:-${AWS_DEFAULT_REGION:-}}}"
# Fall back to the AWS CLI config (honors AWS_PROFILE) when no region env var is set.
if [[ -z "${REGION}" ]]; then REGION="$(aws configure get region 2>/dev/null)" || true; fi
: "${REGION:?set AWS_REGION / AWS_DEFAULT_REGION, run 'aws configure', or pass REGION — required for ECR login + repo operations}"
if [[ -z "${REGISTRY:-}" ]]; then
  # Account ID from AWS_ACCOUNT_ID, else auto-derived from the active AWS credentials.
  if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
    AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)" || true
  fi
  : "${AWS_ACCOUNT_ID:?could not determine AWS account — set AWS_ACCOUNT_ID or REGISTRY, or configure AWS credentials}"
  REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
fi

REPO="${REPO:-ccri/${MODULE}}"
# The repo is IMMUTABLE_WITH_EXCLUSION: every tag is immutable EXCEPT the mutable "floating" tags
# below. SNAPSHOT builds publish to dev-latest (mutable, rolling); cut an immutable release by
# pushing TAG=<version>. The k8s init container that consumes dev-latest must use imagePullPolicy:
# Always (a moved mutable tag is not re-pulled under IfNotPresent).
TAG="${TAG:-dev-latest}"
MUTABLE_TAGS=("dev-latest" "latest")

# Optionally source the carrier JAR(s) from the PUBLISHED Maven artifacts instead of a local
# build.sh dist/ build — for an external image pipeline. Resolved from the repos in settings.xml.
USE_PUBLISHED_JAR="${USE_PUBLISHED_JAR:-false}"
JAR_VERSION="${JAR_VERSION:-}"
MAVEN_REPO_LOCAL="${MAVEN_REPO_LOCAL:-$HOME/.m2/repository}"

# Repo root = the dir above docker/ (this lib lives in docker/).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTEXT="${REPO_ROOT}/dist/${MODULE}"
DOCKERFILE="${REPO_ROOT}/docker/${MODULE}/Dockerfile"
IMAGE="${REGISTRY}/${REPO}:${TAG}"

# Fetch a published JAR (group:artifact:version) into CONTEXT from the configured repositories.
fetch_published_jar() {
  local g="$1" a="$2" v="$3"
  echo ">> Fetching published ${g}:${a}:${v}"
  mvn -q org.apache.maven.plugins:maven-dependency-plugin:3.8.1:get \
    -Dartifact="${g}:${a}:${v}:jar" -Dtransitive=false
  cp "${MAVEN_REPO_LOCAL}/${g//.//}/${a}/${v}/${a}-${v}.jar" "${CONTEXT}/"
}

if [[ "${USE_PUBLISHED_JAR}" == true ]]; then
  [[ -n "${JAR_VERSION}" ]] || { echo "ERROR: USE_PUBLISHED_JAR=true requires JAR_VERSION=<version>" >&2; exit 1; }
  rm -rf "${CONTEXT}"; mkdir -p "${CONTEXT}"
  fetch_published_jar "${GROUP}" "${MODULE}" "${JAR_VERSION}"
  for coord in "${EXTRA_ARTIFACTS[@]:-}"; do
    [[ -n "$coord" ]] || continue
    IFS=':' read -r eg ea ev <<<"$coord"
    fetch_published_jar "$eg" "$ea" "$ev"
  done
fi

if ! ls "${CONTEXT}"/*.jar >/dev/null 2>&1; then
  echo "ERROR: no JAR(s) under ${CONTEXT} — run 'bash build.sh' first, or set USE_PUBLISHED_JAR=true JAR_VERSION=<version>." >&2
  exit 1
fi

echo ">> Logging in to ${REGISTRY}"
aws ecr get-login-password --region "${REGION}" \
  | docker login --username AWS --password-stdin "${REGISTRY}"

# Exclusion-filter args (one per mutable tag) for the IMMUTABLE_WITH_EXCLUSION policy.
EXCLUSION_FILTERS=()
for t in "${MUTABLE_TAGS[@]}"; do EXCLUSION_FILTERS+=("filterType=WILDCARD,filter=${t}"); done

echo ">> Ensuring ECR repo ${REPO} exists"
aws ecr describe-repositories --region "${REGION}" --repository-names "${REPO}" >/dev/null 2>&1 \
  || aws ecr create-repository --region "${REGION}" --repository-name "${REPO}" \
       --image-tag-mutability IMMUTABLE_WITH_EXCLUSION \
       --image-tag-mutability-exclusion-filters "${EXCLUSION_FILTERS[@]}" >/dev/null

# Idempotently enforce the policy (covers a repo that pre-existed with different settings).
echo ">> Enforcing tag mutability: immutable except [${MUTABLE_TAGS[*]}]"
aws ecr put-image-tag-mutability --region "${REGION}" --repository-name "${REPO}" \
  --image-tag-mutability IMMUTABLE_WITH_EXCLUSION \
  --image-tag-mutability-exclusion-filters "${EXCLUSION_FILTERS[@]}" >/dev/null

echo ">> Building ${IMAGE}"
docker build --platform linux/amd64 --provenance=false -f "${DOCKERFILE}" -t "${IMAGE}" "${CONTEXT}"

echo ">> Pushing ${IMAGE}"
docker push "${IMAGE}"

echo ">> Published: ${IMAGE}"
echo ">> Digest:"
docker inspect --format '{{ index .RepoDigests 0 }}' "${IMAGE}" 2>/dev/null || true
