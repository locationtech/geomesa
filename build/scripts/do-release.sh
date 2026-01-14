#!/usr/bin/env bash

set -e
set -u
set -o pipefail

REPOSITORY="locationtech/geomesa"

cd "$(dirname "$0")/../.." || exit

usage() {
  echo "Usage: $(basename "$0") [-h|--help]
where :
  -h| --help Display this help text
" 1>&2
  exit 1
}

if [[ ($# -ne 0) ]]; then
  usage
fi

for ex in mvn curl gpg gh; do
  if ! [[ $(which "$ex") ]]; then
    echo "Error: required executable '$ex' not found"
    exit 1
  fi
done

# get current branch and version we're releasing off
BRANCH="$(git branch --show-current)"
read -r -p "Enter release branch (${BRANCH}): " new_branch
if [[ -n "$new_branch" ]]; then
  if git branch --remotes --list "*/$new_branch"; then
    BRANCH="$new_branch"
  else
    echo "Error: branch '$new_branch' does not exist on remote - may need to \`git fetch\`?"
    exit 1
  fi
fi

VERSION="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"
if ! [[ $VERSION =~ .*-SNAPSHOT ]]; then
  echo "Error: project version is not a SNAPSHOT"
  exit 1
fi
VERSION="${VERSION%-SNAPSHOT}"
read -r -p "Enter release version (${VERSION}): " new_version
if [[ -n "$new_version" ]]; then
  VERSION="$new_version"
fi
TAG="geomesa-${VERSION}"

read -r -p "Releasing version ${VERSION} off branch '${BRANCH}' - continue? (y/n) " confirm
confirm=${confirm,,} # lower-casing
if ! [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
  exit 1
fi

if [[ "$VERSION" =~ .*\.0 ]]; then
  # new major or minor version, bump minor version
  # shellcheck disable=SC2016
  NEXT_VERSION="$(echo '${parsedVersion.majorVersion}.${parsedVersion.nextMinorVersion}.0-SNAPSHOT' \
    | mvn build-helper:parse-version help:evaluate -N -q -DforceStdout -DversionString="$VERSION")"
elif [[ "$VERSION" =~ .*\.[0-9]+ ]]; then
  # bug fix, bump patch version
  # shellcheck disable=SC2016
  NEXT_VERSION="$(echo '${parsedVersion.majorVersion}.${parsedVersion.minorVersion}.${parsedVersion.nextIncrementalVersion}-SNAPSHOT' \
    | mvn build-helper:parse-version help:evaluate -N -q -DforceStdout -DversionString="$VERSION")"
else
  # milestone, rc, etc, go back to original dev version
  # shellcheck disable=SC2016
  NEXT_VERSION="$(echo '${parsedVersion.majorVersion}.${parsedVersion.minorVersion}.${parsedVersion.incrementalVersion}-SNAPSHOT' \
    | mvn build-helper:parse-version help:evaluate -N -q -DforceStdout -DversionString="$VERSION")"
fi

echo "Validating gh access"
gh auth status >/dev/null

# TODO validate sonatype auth token
SONATYPE_AUTH="Authorization: Bearer $(printf '%s:%s' \
  "$(grep -A2 '<id>sonatype</id>' ~/.m2/settings.xml | grep username | sed 's| *<username>\(.*\)</username>|\1|')" \
  "$(grep -A2 '<id>sonatype</id>' ~/.m2/settings.xml | grep password | sed 's| *<password>\(.*\)</password>|\1|')" \
  | base64)"

# trigger the release workflow
echo "Triggering GitHub release workflow"
gh workflow run cut-release.yml \
  --repo "${REPOSITORY}" \
  --ref "${BRANCH}" \
  -f "version=${VERSION}" \
  -f "next_version=${NEXT_VERSION}"

echo -n "Waiting for run to start "
run_id=""
while true; do
  echo -n "."
  run_id="$(gh run list \
    --repo "${REPOSITORY}" \
    --branch "${BRANCH}" \
    --workflow cut-release.yml \
    --jq ".[].databaseId" \
    --json 'databaseId' \
    --limit 1)"
  if [ -n "${run_id}" ]; then
    echo ""
    break
  fi
  sleep 10
done

echo "Found run ${run_id} - waiting for run to finish"
gh run watch "${run_id}" \
  --repo "${REPOSITORY}" \
  --exit-status \
  --interval 60

echo -n "Waiting for GitHub release build ${TAG} run to start "
run_id=""
while true; do
  echo -n "."
  run_id="$(gh run list \
    --repo "${REPOSITORY}" \
    --branch "${TAG}" \
    --workflow build-release.yml \
    --jq ".[].databaseId" \
    --json 'databaseId' \
    --limit 1)"
  if [ -n "${run_id}" ]; then
    echo ""
    break
  fi
  sleep 10
done

echo "Found run ${run_id} - waiting for run to finish"
gh run watch "${run_id}" \
  --repo "${REPOSITORY}" \
  --exit-status \
  --interval 60

echo "Downloading artifacts from GitHub release"
mkdir "${VERSION}"
gh release download "${TAG}" \
  --repo "${REPOSITORY}" \
  --dir "${VERSION}" \
  --skip-existing

echo "Signing binary artifacts"
while IFS= read -r -d '' file; do
  pushd "$(dirname "$file")" >/dev/null
  gpg --armor --detach-sign "$(basename "$file")"
  gh release upload "${TAG}" \
    --repo "${REPOSITORY}" \
    "$(basename "$file").asc"
  sleep 1
  popd >/dev/null
done < <(find "${VERSION}" -name '*-bin.tar.gz' -print0)

echo "Merging Maven release bundles"
mkdir "${VERSION}-staging"
for scala_version in 2.13 2.12; do
  tar -xf "${VERSION}/${TAG}_${scala_version}-staging.tgz" -C "${VERSION}-staging"
done

echo "Signing Maven artifacts"
while IFS= read -r -d '' file; do
  pushd "$(dirname "$file")" >/dev/null
  gpg --armor --detach-sign "$(basename "$file")"
  popd >/dev/null
done < <(find "${VERSION}-staging" -not -name '*.sha1' -not -name '*.sha256' -not -name '*.sha512' -not -name '*.md5' -print0)

echo "Uploading Maven bundle"
tar -czf "${VERSION}-staging.tgz" -C "${VERSION}-staging" .

exit 0

deployment_id="$(curl --request POST \
  --verbose \
  --header "${SONATYPE_AUTH}" \
  --form "bundle=@${VERSION}-staging.tgz" \
  --form "name=${TAG}" \
  --form "publishingType=USER_MANAGED" \
  https://central.sonatype.com/api/v1/publisher/upload)"

# TODO once we've verified the release process works correctly, can set publishingType=AUTOMATIC and wait for deploymentState=PUBLISHED

echo "Waiting for Maven bundle to validate"
deployment_state=PENDING # valid states: PENDING VALIDATING VALIDATED PUBLISHING PUBLISHED FAILED
while [[ $deployment_state =~ PENDING|VALIDATING ]]; do
  sleep 10
  deployment_state="$(curl --request POST \
    --header "${SONATYPE_AUTH}" \
    "https://central.sonatype.com/api/v1/publisher/status?id=${deployment_id}" \
    | jq '.deploymentState')"
done

if [[ $deployment_state != VALIDATED ]]; then
  echo "Deployment failed to validate - status is ${deployment_state}"
  exit 1
fi

echo "Deleting Maven artifacts from GitHub release"
for scala_version in 2.13 2.12; do
  gh release delete-asset "${TAG}" "${TAG}_${scala_version}-staging.tgz" \
    --repo locationtech/geomesa \
    --yes
done

echo "Publishing GitHub release"
gh release edit "${TAG}" \
  --repo "${REPOSITORY}" \
  --draft=false

echo "Done"
