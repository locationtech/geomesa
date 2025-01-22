#!/usr/bin/env bash

set -e
set -u
set -o pipefail

cd "$(dirname "$0")/../.." || exit

# update as needed for your local env
WEBSITE_DIR="$(pwd)/../geomesa.github.io"

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

if ! [[ -d "$WEBSITE_DIR" ]]; then
  echo "Error: required directory $WEBSITE_DIR does not exist or is not readable"
  exit 1
fi

JAVA_VERSION="$(mvn help:evaluate -Dexpression=jdk.version -q -DforceStdout)"
if ! [[ $(java -version 2>&1 | head -n 1 | cut -d'"' -f2) =~ ^$JAVA_VERSION.* ]]; then
  echo "Error: invalid Java version - Java $JAVA_VERSION required"
  exit 1
fi

if ! [[ $(which virtualenv) ]]; then
  echo "Error: virtualenv executable not found (required for building docs)"
  exit 1
fi

TAG="$(git tag | grep '^geomesa-' | sort -r | head -n 1)"
RELEASE="${TAG#geomesa-}"
read -r -p "Enter the release version (default $RELEASE): " REL
if [[ -n "$REL" ]]; then
  RELEASE="$REL"
  TAG="geomesa-$REL"
fi

LAST_RELEASE="$(grep VERSION: "$WEBSITE_DIR"/documentation/stable/_static/documentation_options.js | awk '{ print $2 }' | sed "s/[', ]//g")"
if [[ -z "$LAST_RELEASE" ]]; then
  echo "Error: unable to determine last release version"
  exit 1
fi
LAST_RELEASE_SHORT="${LAST_RELEASE%.*}"

# get current branch we're on
BRANCH="$(git branch --show-current)"
git checkout "$TAG"

# configure virtualenv for building the docs
# shellcheck disable=SC1091
virtualenv .sphinx && source .sphinx/bin/activate && pip install -r docs/requirements.txt

# build the docs
mvn clean install -pl docs/ -Pdocs

# move current docs to a versioned folder
pushd "$WEBSITE_DIR" && \
  git checkout main && \
  git pull origin main && \
  mv documentation/stable "documentation/${LAST_RELEASE_SHORT}" && \
  mkdir documentation/stable/ && \
  popd

# copy the docs we just built
cp -r docs/target/html/* "$WEBSITE_DIR"/documentation/stable/

# build site docs - takes ~an hour
mvn clean package scoverage:integration-test -Pscoverage -Dmaven.source.skip && \
  mvn generate-sources site && \
  mvn site:stage -DstagingDirectory="$WEBSITE_DIR"/documentation/stable/site/

# update 'stable version'
sed -i "s/^stableVersion:.*/stableVersion: \"$RELEASE\"/" "$WEBSITE_DIR"/_config.yml

# add link to previous docs
sed -i "s|<li class=\"dropdown-header\">Previous Releases</li>|\0\n            <li><a href=\"/documentation/${LAST_RELEASE_SHORT}/\">${LAST_RELEASE}</a></li>|" "$WEBSITE_DIR"/_includes/header.html

# commit and push docs
pushd "$WEBSITE_DIR" && \
  git add documentation && \
  git add -u && \
  git commit -m "Updating docs for ${RELEASE}" && \
  git push origin main && \
  popd

# exit virtualenv
deactivate

# reset back to original branch
git checkout "$BRANCH"
