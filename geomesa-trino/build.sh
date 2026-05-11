#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DEPLOY_IMAGES=false
PUBLISH_JARS=false
CLI_VERSION=""
CLI_TAG=""
CLI_REGISTRY=""
CLI_REPO=""
usage() {
  cat >&2 <<'EOF'
Usage: build.sh [--publish-jars] [-d|--deploy-images] [-v|--version VERSION]
                [-t|--tag TAG] [-r|--registry REGISTRY] [--repo ID::URL]

Builds the geomesa-trino-plugin (fat JAR) and geomesa-trino-datastore carrier JARs
into dist/, via the GeoMesa monorepo reactor. Maven runs on the standard JDK 17; only
geomesa-trino-plugin's javac is forked to a JDK 25 toolchain (the datastore builds on JDK 17),
which must be configured first (run build/scripts/update-maven-toolchains.sh once). This script
verifies it and aborts if absent.

      --publish-jars    On build success, 'mvn deploy' the versioned plugin + datastore
                        JARs to the configured Maven repository. JARs deploy at their POM
                        version; the repo + credentials come from the project's
                        distributionManagement / settings.xml (or --repo).
  -d, --deploy-images   Build + push the carrier Docker images (runs docker/*/deploy.sh),
                        then print the helmfile image lines. IMPLIES --publish-jars.
                        Needs docker + registry credentials.
  -v, --version VER     Version label used as the image tag (default: deploy.sh default,
                        dev-latest). Does NOT change the published JAR version — that comes
                        from the POM; cut a release via the normal release process.
  -t, --tag TAG         Image-tag override; falls back to --version when unset.
  -r, --registry REG    Image registry (overrides the REGISTRY env var).
      --repo ID::URL    Maven deploy target for --publish-jars (-> -DaltDeploymentRepository);
                        defaults to the project's distributionManagement / settings.xml.
  -h, --help            Show this help.

  -v/-t/-r and the printed image lines only apply with --deploy-images.
EOF
}

# Assert a value-taking flag ("$@" = flag + remaining args) was actually given its value.
need() { [[ $# -ge 2 ]] || { echo "build.sh: $1 requires a value" >&2; exit 2; }; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    -d|--deploy-images) DEPLOY_IMAGES=true; shift ;;
    --publish-jars)     PUBLISH_JARS=true; shift ;;
    -v|--version)       need "$@"; CLI_VERSION="$2"; shift 2 ;;
    -t|--tag)           need "$@"; CLI_TAG="$2"; shift 2 ;;
    -r|--registry)      need "$@"; CLI_REGISTRY="$2"; shift 2 ;;
    --repo)             need "$@"; CLI_REPO="$2"; shift 2 ;;
    -h|--help)          usage; exit 0 ;;
    *) echo "build.sh: unknown argument '$1'" >&2; usage; exit 2 ;;
  esac
done

# --deploy-images builds the carrier image from the published JARs, so it implies --publish-jars.
if [[ "$DEPLOY_IMAGES" == true ]]; then PUBLISH_JARS=true; fi

ROOT_POM="$SCRIPT_DIR/../pom.xml"
TRINO_MODULES="geomesa-trino/geomesa-trino-plugin,geomesa-trino/geomesa-trino-datastore"

PLUGIN_DIR="dist/geomesa-trino-plugin"
DATASTORE_DIR="dist/geomesa-trino-datastore"

# Stage a module's runtime JAR into its dist dir, dropping the shade 'original-*' jar and the
# sources/javadoc jars. The carrier images COPY *.jar from these dirs, so each holds exactly the
# runtime JAR(s); the rm clears any prior (incl. older module-named) jar first.
stage_jars() {
  local module="$1" dest="$2"
  mkdir -p "$dest"
  rm -f "$dest"/*.jar
  find "$module/target/" -maxdepth 1 -name "$module-*.jar" \
    ! -name "original-*" ! -name "*-sources.jar" ! -name "*-javadoc.jar" -exec cp {} "$dest"/ \;
}

# Image reference to record in the helmfile: name:tag for an explicit tag, else the pushed
# digest (name@sha256:...) parsed from the deploy log — empty if a digest wasn't captured.
resolve_image() {
  local name="$1" log="$2" tag="$3"
  if [[ -n "$tag" ]]; then
    echo "${name}:${tag}"
  else
    grep -oE "${name}@sha256:[0-9a-f]{64}" "$log" | tail -1 || true
  fi
}

REQ_JDK="$(grep -oE '<trino.jdk.version>[0-9]+' "$ROOT_POM" | grep -oE '[0-9]+' | head -1 || true)"
REQ_JDK="${REQ_JDK:-25}"
toolchain_has_jdk() { [[ -f "$1" ]] && grep -q "<version>${REQ_JDK}</version>" "$1"; }
if ! { toolchain_has_jdk "$HOME/.m2/toolchains.xml" \
       || toolchain_has_jdk "${MAVEN_HOME:-${M2_HOME:-/nonexistent}}/conf/toolchains.xml"; }; then
  echo "ERROR: no JDK ${REQ_JDK} Maven toolchain is configured." >&2
  echo "       geomesa-trino-plugin compiles against Trino's Java ${REQ_JDK} bytecode, so its javac is forked" >&2
  echo "       to a JDK ${REQ_JDK} toolchain (Maven itself can stay on JDK 17). Configure it once with:" >&2
  echo "         build/scripts/update-maven-toolchains.sh" >&2
  exit 1
fi

echo "Building geomesa-trino-plugin (fat JAR) + geomesa-trino-datastore via the geomesa reactor..."
# install (not just package) so the reactor deps (e.g. geomesa-z3) are in the local repo for
# the scoped 'deploy' below.
mvn -f "$ROOT_POM" -pl "$TRINO_MODULES" -am clean install -q -DskipTests -Dlicense.skip=true

# Stage the connector fat JAR + the datastore carrier JAR into dist/.
stage_jars geomesa-trino-plugin    "$PLUGIN_DIR"
stage_jars geomesa-trino-datastore "$DATASTORE_DIR"
# The datastore image also carries the Trino JDBC driver.
mvn -f "$ROOT_POM" -q -pl geomesa-trino/geomesa-trino-datastore dependency:copy-dependencies \
  -DincludeArtifactIds=trino-jdbc -DoutputDirectory="$SCRIPT_DIR/$DATASTORE_DIR"

echo ""
echo "Plugin JAR ready in $PLUGIN_DIR/; datastore JARs ready in $DATASTORE_DIR/"

# --- Publish the versioned JARs to the Maven repository ---
if [[ "$PUBLISH_JARS" == true ]]; then
  echo ""
  echo ">> --publish-jars: deploying plugin + datastore JARs (POM version) to the Maven repo"
  DEPLOY_ARGS=(-f "$ROOT_POM" -pl "$TRINO_MODULES" deploy -DskipTests -Dlicense.skip=true
               -Daether.connector.http.retryHandler.count=5
               -Daether.connector.http.reuseConnections=false)
  if [[ -n "$CLI_REPO" ]]; then
    DEPLOY_ARGS+=("-DaltDeploymentRepository=$CLI_REPO")
    echo "   target: $CLI_REPO"
  else
    echo "   target: project distributionManagement / settings.xml"
  fi
  mvn "${DEPLOY_ARGS[@]}"
  echo "   published org.locationtech.geomesa:{geomesa-trino-plugin,geomesa-trino-datastore}"
fi

# --- Build + push carrier Docker images ---
if [[ "$DEPLOY_IMAGES" == true ]]; then
  echo ""
  echo ">> --deploy-images: building + pushing carrier images"
  # Image-tag precedence: -t/--tag override > -v/--version > deploy.sh default (dev-latest).
  APPLIED_TAG="${CLI_TAG:-$CLI_VERSION}"
  if [[ -n "$APPLIED_TAG" ]]; then export TAG="$APPLIED_TAG"; fi
  if [[ -n "$CLI_REGISTRY" ]]; then export REGISTRY="$CLI_REGISTRY"; fi
  echo "   registry=${REGISTRY:-<deploy.sh default>}  tag=${APPLIED_TAG:-<deploy.sh default>}"
  # tee each deploy's output (progress stays live) into a log resolve_image parses for the
  # pushed digest. set -o pipefail + set -e abort if a deploy fails, so the summary only
  # prints on success.
  PLUGIN_LOG="$(mktemp)"; DATASTORE_LOG="$(mktemp)"
  trap 'rm -f "$PLUGIN_LOG" "$DATASTORE_LOG"' EXIT
  bash "$SCRIPT_DIR/docker/geomesa-trino-plugin/deploy.sh"    | tee "$PLUGIN_LOG"
  bash "$SCRIPT_DIR/docker/geomesa-trino-datastore/deploy.sh" | tee "$DATASTORE_LOG"

  plugin_image="$(resolve_image    geomesa-trino-plugin    "$PLUGIN_LOG"    "$APPLIED_TAG")"
  datastore_image="$(resolve_image geomesa-trino-datastore "$DATASTORE_LOG" "$APPLIED_TAG")"

  echo ""
  echo "=================================================================================="
  echo " Helmfiles deployment- update queryApi.trino.{plugin,datastore}.image in"
  echo " _environments/<env>/20-shared.yaml.gotmpl:"
  echo ""
  echo "    plugin:"
  echo "      image: \"${plugin_image:-<digest not captured; run docker inspect on the pushed image>}\""
  echo "    datastore:"
  echo "      image: \"${datastore_image:-<digest not captured; run docker inspect on the pushed image>}\""
  echo "=================================================================================="
fi

if [[ "$PUBLISH_JARS" != true ]]; then
  echo "Run: docker compose -f docker-compose.local.yml restart trino"
  echo "     (or docker-compose.aws.yml — see README 'Environments' section)"
  echo "Then verify: docker compose -f docker-compose.local.yml logs trino | grep -i spatial"
  echo "Publish JARs with:  bash build.sh --publish-jars"
  echo "Build+push images:  bash build.sh --deploy-images   (implies --publish-jars)"
fi
