#!/usr/bin/env bash

set -e
set -u
set -o pipefail

cd "$(dirname "$0")/../.." || exit

checkExecutables() {
  local err=""
  for ex in git mvn curl gpg gh jq sha256sum; do
    if ! [[ $(which "$ex") ]]; then
      err="$err, '$ex'"
    fi
  done
  if [[ -n "$err" ]]; then
    echo "Error: required executable(s) ${ex:2} not found - please install them and re-run"
    exit 1
  fi
}

checkExecutables

REPOSITORY="locationtech/geomesa"
# the indentation only matches the top-level version tag
VERSION="$(grep '^    <version>' pom.xml | head -n1 | sed -E 's|.*<version>(.*)</version>.*|\1|')"
if [[ $VERSION =~ .*-SNAPSHOT ]]; then
  VERSION="${VERSION%-SNAPSHOT}"
else
  VERSION=""
fi

usage() {
  echo "Usage: $(basename "$0") [--help] [--repo REPOSITORY] [--version VERSION]
flags:
  --help               Display this usage
  --version <VERSION>  The version to release (default: $VERSION)
  --repo <REPOSITORY>  The GitHub repository used to run the release (default: $REPOSITORY)
  --branch <BRANCH>    The git branch to release off (default depends on the release version)
" 1>&2
}

# parse command line arguments
version_flag=""
branch_flag=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --help)
      usage
      exit 0
      ;;
    --repo)
      if [[ -n "$2" && "$2" != -* ]]; then
        REPOSITORY="$2"
        shift 2
      else
        echo "Error: --repo requires a repository argument" >&2
        usage
        exit 1
      fi
      ;;
    --version)
      if [[ -n "$2" && "$2" != -* ]]; then
        version_flag="$2"
        shift 2
      else
        echo "Error: --version requires a version argument" >&2
        usage
        exit 1
      fi
      ;;
    --branch)
      if [[ -n "$2" && "$2" != -* ]]; then
        branch_flag="$2"
        shift 2
      else
        echo "Error: --branch requires a repository argument" >&2
        usage
        exit 1
      fi
      ;;
    *)
      echo "Error: Unknown option $1" >&2
      usage
      exit 1
      ;;
  esac
done

# Progress wheel spinner
spin_chars='\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-\|/-'
spin_index=0
spin() {
  printf "\b%s" "${spin_chars:$spin_index:1}"
  spin_index=$(( (spin_index + 1) % 100 ))
}
# rotate the spinner and sleep
# args: $1 - how many seconds will pass before spin_index = 0, max 10
spin_sleep() {
  local mod="${1}0"
  printf "\b%s" "${spin_chars:$spin_index:1}"
  spin_index=$(( (spin_index + 1) % mod ))
  sleep 0.1
}

if [[ -n "${version_flag}" ]]; then
  VERSION="${version_flag}"
elif [[ -z "${VERSION}" ]]; then
  while [[ -z "${VERSION}" ]]; do
    read -r -p "Enter release version: " VERSION
  done
else
  read -r -p "Enter release version: (${VERSION}) " new_version
  if [[ -n "$new_version" ]]; then
    VERSION="$new_version"
  fi
fi

TAG="geomesa-${VERSION}"
RELEASE_DIR="${VERSION}"
STAGING_DIR="${VERSION}/staging"

if [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.0$ ]]; then
  # new major or minor version, bump minor version
  # shellcheck disable=SC2016
  NEXT_VERSION="$(echo '${parsedVersion.majorVersion}.${parsedVersion.nextMinorVersion}.0-SNAPSHOT' \
    | mvn build-helper:parse-version help:evaluate -N -q -DforceStdout -DversionString="$VERSION")"
  BRANCH="main"
elif [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  # bug fix, bump patch version
  # shellcheck disable=SC2016
  NEXT_VERSION="$(echo '${parsedVersion.majorVersion}.${parsedVersion.minorVersion}.${parsedVersion.nextIncrementalVersion}-SNAPSHOT' \
    | mvn build-helper:parse-version help:evaluate -N -q -DforceStdout -DversionString="$VERSION")"
  BRANCH="${NEXT_VERSION%.*}.x"
else
  # milestone, rc, etc, go back to original dev version
  # shellcheck disable=SC2016
  NEXT_VERSION="$(echo '${parsedVersion.majorVersion}.${parsedVersion.minorVersion}.${parsedVersion.incrementalVersion}-SNAPSHOT' \
    | mvn build-helper:parse-version help:evaluate -N -q -DforceStdout -DversionString="$VERSION")"
  BRANCH="main"
fi

if [[ -n "${branch_flag}" ]]; then
  BRANCH="${branch_flag}"
else
  read -r -p "Enter branch to release off of: (${BRANCH}) " new_branch
  if [[ -n "$new_branch" ]]; then
    BRANCH="$new_branch"
  fi
fi

# ensure the branch exists
git fetch -q "git@github.com:${REPOSITORY}.git" "$BRANCH"

read -r -p "Releasing version ${VERSION} off branch '${BRANCH}' in repository https://github.com/${REPOSITORY} - continue? (Yes) " confirm
confirm=${confirm,,} # lower-casing
if ! [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
  exit 0
fi

if [[ -d "${RELEASE_DIR}" ]]; then
  read -r -p "Found existing release directory ${RELEASE_DIR} - to delete it, enter DELETE: " confirm
  if [[ "$confirm" == "DELETE" ]]; then
    rm -rf "${RELEASE_DIR}"
    echo "Deleted ${RELEASE_DIR}"
  else
    echo "Please rename, move or delete the directory and re-run"
    exit 1
  fi
fi

# api token needs: all repositories (or selected to include this one), actions r/w, contents r/w
echo "Validating gh access"
gh auth status >/dev/null

echo "Validating sonatype access"
# TODO validate sonatype auth token
SONATYPE_AUTH="Authorization: Bearer $(printf '%s:%s' \
  "$(grep -A2 '<id>sonatype</id>' ~/.m2/settings.xml | grep username | sed 's| *<username>\(.*\)</username>|\1|')" \
  "$(grep -A2 '<id>sonatype</id>' ~/.m2/settings.xml | grep password | sed 's| *<password>\(.*\)</password>|\1|')" \
  | base64)"

echo "Triggering release tagging workflow"
start_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
gh workflow run tag-release.yml \
  --repo "${REPOSITORY}" \
  --ref "${BRANCH}" \
  -f "version=${VERSION}" \
  -f "next_version=${NEXT_VERSION}"

echo -n "Waiting for release tagging run to start  "
run_id=""
while true; do
  spin_sleep 2
  if [[ spin_index -eq 0 ]]; then
    run_id="$(gh run list \
      --repo "${REPOSITORY}" \
      --branch "${BRANCH}" \
      --workflow tag-release.yml \
      --jq ".[] | select(.createdAt >= \"${start_utc}\") | .databaseId" \
      --json 'databaseId,createdAt' \
      --limit 1)"
    if [ -n "${run_id}" ]; then
      echo ""
      break
    fi
  fi
done

echo "Found run ${run_id} - waiting for run to finish"
gh run watch "${run_id}" \
  --repo "${REPOSITORY}" \
  --exit-status \
  --interval 10

echo "Triggering release build workflow"
start_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
gh workflow run build-release.yml \
  --repo "${REPOSITORY}" \
  --ref "${TAG}"

echo -n "Waiting for release build run to start  "
run_id=""
while true; do
  spin_sleep 2
  if [[ spin_index -eq 0 ]]; then
    run_id="$(gh run list \
      --repo "${REPOSITORY}" \
      --branch "${TAG}" \
      --workflow build-release.yml \
      --jq ".[] | select(.createdAt >= \"${start_utc}\") | .databaseId" \
      --json 'databaseId,createdAt' \
      --limit 1)"
    if [ -n "${run_id}" ]; then
      echo ""
      break
    fi
  fi
done

echo "Found run ${run_id} - waiting for run to finish"
gh run watch "${run_id}" \
  --repo "${REPOSITORY}" \
  --exit-status \
  --interval 10

echo "Downloading artifacts from GitHub release"
mkdir "${RELEASE_DIR}"
gh release download "${TAG}" \
  --repo "${REPOSITORY}" \
  --dir "${RELEASE_DIR}" \
  --skip-existing

echo "Extracting Maven release bundles"
mkdir "${STAGING_DIR}"
for scala_version in 2.13 2.12; do
  tar -xf "${RELEASE_DIR}/${TAG}_${scala_version}-staging.tgz" -C "${STAGING_DIR}"
done

echo "Verifying downloaded artifacts  "
while IFS= read -r -d '' file; do
  spin
  pushd "$(dirname "$file")" >/dev/null
  sha256sum -c "$(basename "$file")" >/dev/null
  popd >/dev/null
done < <(find "${RELEASE_DIR}" -maxdepth 1 -type f -name '*.sha256' -print0)
while IFS= read -r -d '' file; do
  spin
  echo "$(cat "$file") $file" | sha256sum -c >/dev/null
done < <(find "${STAGING_DIR}" -type f -name '*.sha256' -print0)

echo "Signing binary artifacts  "
while IFS= read -r -d '' file; do
  spin
  gpg --armor --detach-sign "$file"
done < <(find "${RELEASE_DIR}" -maxdepth 1 -name '*-bin.tar.gz' -print0)
echo ""

echo "Uploading signatures to GitHub release"
# shellcheck disable=SC2046
gh release upload "${TAG}" \
  --repo "${REPOSITORY}" \
  $(find "${RELEASE_DIR}" -maxdepth 1 -name '*-bin.tar.gz.asc')

echo -n "Signing Maven artifacts  "
while IFS= read -r -d '' file; do
  spin
  gpg --armor --detach-sign "$file"
done < <(find "${STAGING_DIR}" -type f -not -name '*.sha1' -not -name '*.sha256' -not -name '*.sha512' -not -name '*.md5' -print0)
echo ""

echo "Creating Maven bundle for upload"
# note: can't have a leading "./" in the path names inside the tar file, or sonatype validation will fail
tar -czf "${STAGING_DIR}.tgz" -C "${STAGING_DIR}" org

echo "Uploading Maven bundle to sonatype"
curl \
  --header "${SONATYPE_AUTH}" \
  --form "bundle=@${STAGING_DIR}.tgz" \
  --form "name=${TAG}" \
  --form "publishingType=USER_MANAGED" \
  --progress-bar \
  -o .deployment_id \
  https://central.sonatype.com/api/v1/publisher/upload
deployment_id="$(cat .deployment_id)"

echo -n "Deployment ${deployment_id} submitted - waiting for deployment to publish "
# TODO once we've verified the release process works correctly, can set publishingType=AUTOMATIC and wait for deploymentState=PUBLISHED
deployment_state=PENDING # valid states: PENDING VALIDATING VALIDATED PUBLISHING PUBLISHED FAILED
while [[ $deployment_state =~ PENDING|VALIDATING ]]; do
  spin_sleep 5
  if [[ spin_index -eq 0 ]]; then
    deployment_state="$(curl \
      --silent \
      --show-error \
      --request POST \
      --header "${SONATYPE_AUTH}" \
      "https://central.sonatype.com/api/v1/publisher/status?id=${deployment_id}" \
      | jq '.deploymentState' | sed "s/[\"']//g")"
  fi
done
echo ""

# TODO PUBLISHED
if [[ "${deployment_state}" != VALIDATED ]]; then
  echo "Deployment failed to publish - status is ${deployment_state}"
  exit 1
fi
rm .deployment_id

echo "Deleting Maven artifacts from GitHub release"
for scala_version in 2.13 2.12; do
  gh release delete-asset "${TAG}" "${TAG}_${scala_version}-staging.tgz" \
    --repo "${REPOSITORY}" \
    --yes
done

echo "Publishing GitHub release"
gh release edit "${TAG}" \
  --repo "${REPOSITORY}" \
  --draft=false

echo "Triggering release documentation workflow"
start_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
gh workflow run release-docs.yml \
  --repo "geomesa/geomesa.github.io" \
  --ref main \
  -f "tag=${TAG}"

echo -n "Waiting for documentation run to start "
run_id=""
while true; do
  spin_sleep 2
  if [[ spin_index -eq 0 ]]; then
    run_id="$(gh run list \
      --repo "geomesa/geomesa.github.io" \
      --branch main \
      --workflow release-docs.yml \
      --jq ".[] | select(.createdAt >= \"${start_utc}\") | .databaseId" \
      --json 'databaseId,createdAt' \
      --limit 1)"
    if [ -n "${run_id}" ]; then
      echo ""
      break
    fi
  fi
done

echo "Found run ${run_id} - waiting for run to finish"
gh run watch "${run_id}" \
  --repo "geomesa/geomesa.github.io" \
  --exit-status \
  --interval 10

echo "Release complete"
