#!/usr/bin/env bash

# Determines whether a pull request's changed files affect a given CI matrix job.
#
# A job is affected if any changed file belongs to a module in the job's Maven build closure -
# the modules it builds (-pl) plus all of their transitive upstream dependencies (-am). Since a
# change to a shared module (e.g. geomesa-utils) is in the closure of every dependent job, those
# jobs are correctly reported as affected.
#
# Prints "true" if the job is affected (or if we can't tell, to fail safe), otherwise "false".
#
# Usage: detect-affected-modules.sh <base-sha> <head-sha> <module-list>
# where <module-list> is the matrix 'list' value, e.g. "geomesa-fs/geomesa-fs-spark".

set -e

if [[ $# -ne 3 ]]; then
  echo "Usage: $(basename "$0") <base-sha> <head-sha> <module-list>" 1>&2
  exit 1
fi

BASE="$1"
HEAD="$2"
LIST="$3"

# make sure both commits are present, then diff them (two-dot, only needs the two tree objects)
git fetch --no-tags --depth=1 origin "$BASE" "$HEAD" 1>&2
CHANGED="$(git diff --name-only "$BASE" "$HEAD")"

# if we can't determine what changed, run everything
if [[ -z "$CHANGED" ]]; then
  echo "true"
  exit 0
fi

# all reactor module directories, relative to the repo root (root pom -> empty string)
mapfile -t ALLMODS < <(find . -name pom.xml -not -path '*/target/*' -not -path '*/src/*' -printf '%h\n' | sed 's|^\./\?||' | sort)

# the -am build closure for this job, as repo-relative directories, one per line.
# we use the 'validate' phase and parse the "  from <dir>/pom.xml" lines maven prints for each
# reactor module - unlike exec:exec, this does not resolve dependencies, so it avoids downloading
# (or failing to find) the unbuilt inter-module geomesa snapshot jars, which aren't in the cache.
CLOSURE="$(mvn $MAVEN_CLI_OPTS -pl $LIST -am validate 2>/dev/null \
  | sed -n 's|^\[INFO\]   from \(.*\)pom\.xml$|\1|p' | sed 's|/$||' | sort -u)"

# resolve a file to its owning module: the longest module dir that prefixes the file path
owning_module() {
  local file="$1" owner="" mod
  for mod in "${ALLMODS[@]}"; do
    if [[ -z "$mod" ]]; then
      continue
    fi
    if [[ "$file" == "$mod/"* && ${#mod} -gt ${#owner} ]]; then
      owner="$mod"
    fi
  done
  echo "$owner"
}

while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  owner="$(owning_module "$file")"
  # a file with no owning module belongs to the repo root (root pom, build/, .github/, ...),
  # which is in every job's closure - so treat it as affecting everything
  if [[ -z "$owner" ]] || grep -qxF "$owner" <<< "$CLOSURE"; then
    echo "true"
    exit 0
  fi
done <<< "$CHANGED"

echo "false"
