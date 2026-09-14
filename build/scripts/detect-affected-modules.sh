#!/usr/bin/env bash

# Determines whether a pull request's changed files affect a given CI matrix job.
#
# A job is affected if any changed file belongs to a module in the job's Maven build closure -
# the modules it builds (-pl) plus all of their transitive upstream dependencies (-am). Since a
# change to a shared module (e.g. geomesa-utils) is in the closure of every dependent job, those
# jobs are correctly reported as affected.
#
# The closure is computed directly from the reactor's pom.xml files (parent references and
# inter-module <dependencies>), NOT by invoking maven. This is fast, needs no network, and does
# not resolve snapshot artifacts - so it keeps working right after a version bump, before the new
# snapshots have been published. Anything that can't be resolved unambiguously (an unknown module
# in the list, an unparseable pom, a geomesa dependency that matches no reactor module) is treated
# as an error and fails the step, rather than silently guessing.
#
# Prints "true" if the job is affected, otherwise "false".
#
# Usage: detect-affected-modules.sh <base-sha> <head-sha> <module-list>
# where <module-list> is the matrix 'list' value, e.g. "geomesa-fs/geomesa-fs-spark".

# -f disables filename globbing: we word-split space-separated artifactId lists below and never
# rely on globbing, so this prevents a stray token (e.g. an exclusion '*') from expanding to paths.
set -eo pipefail -f

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

# extract a single pom's own artifactId, its parent artifactId, and its inter-module dependency
# artifactIds. we deliberately ignore:
#   <dependencyManagement> - the root pom lists every module there; these are not real -am edges
#   <build>/<plugins>      - plugin dependencies, not reactor edges
#   <exclusions>           - excluded transitives (and their '*' wildcards), not edges
#   <profiles>             - off by default, so not in maven's default -am closure
# ${scala.binary.version} is the only placeholder used in module artifactIds, so we resolve it to
# the value CI builds. output lines: "OWN<tab>id", "PARENT<tab>id" (optional), "DEP<tab>id" (repeated).
parse_pom() {
  awk '
    /<parent>/{p=1} /<\/parent>/{p=0}
    /<dependencyManagement>/{dm=1} /<\/dependencyManagement>/{dm=0}
    /<dependencies>/{d++} /<\/dependencies>/{d--}
    /<build>/{b++} /<\/build>/{b--}
    /<exclusions>/{x++} /<\/exclusions>/{x--}
    /<profiles>/{pr++} /<\/profiles>/{pr--}
    {
      while (match($0, /<artifactId>[^<]+<\/artifactId>/)) {
        id = substr($0, RSTART+12, RLENGTH-25)
        $0 = substr($0, RSTART+RLENGTH)
        gsub(/\$\{scala.binary.version\}/, "2.12", id)
        if (x > 0 || pr > 0) continue                              # ignore <exclusions>/<profiles> ids
        if (p) { if (parent == "") parent = id; continue }
        if (d >= 1 && dm == 0 && b == 0) { deps[id] = 1; continue }
        if (d == 0 && dm == 0 && b == 0 && own == "") own = id
      }
    }
    END {
      print "OWN\t" own
      if (parent != "") print "PARENT\t" parent
      for (k in deps) print "DEP\t" k
    }
  ' "$1"
}

# all reactor poms, sorted for stable iteration
mapfile -t POMS < <(find . -name pom.xml -not -path '*/target/*' -not -path '*/src/*' | sort)

declare -A ARTIFACT_DIR   # artifactId -> repo-relative module dir ("." for the root pom)
declare -A IS_DIR         # set of known reactor module dirs
DIRS=()                   # parallel arrays, one entry per pom
PARENTIDS=()
DEPIDS=()

# pass 1: parse every pom, record its own artifactId -> dir mapping
for pom in "${POMS[@]}"; do
  # dirname gives "." for the root pom; keep that as the root's marker (bash associative
  # arrays cannot key on an empty string). the root dir never owns a changed file directly.
  dir="$(dirname "$pom")"
  dir="${dir#./}"

  out="$(parse_pom "$pom")"
  own="$(sed -n 's/^OWN\t//p' <<< "$out")"
  if [[ -z "$own" ]]; then
    echo "$(basename "$0"): could not determine artifactId from $pom" 1>&2
    exit 1
  fi
  if [[ -n "${ARTIFACT_DIR[$own]+x}" ]]; then
    echo "$(basename "$0"): duplicate artifactId '$own' (${ARTIFACT_DIR[$own]:-<root>} and ${dir:-<root>})" 1>&2
    exit 1
  fi

  ARTIFACT_DIR["$own"]="$dir"
  IS_DIR["$dir"]=1
  DIRS+=("$dir")
  PARENTIDS+=("$(sed -n 's/^PARENT\t//p' <<< "$out")")
  DEPIDS+=("$(sed -n 's/^DEP\t//p' <<< "$out" | tr '\n' ' ')")
done

# pass 2: resolve parent + dependency artifactIds to dirs, building the upstream adjacency.
# a geomesa-* reference that resolves to no reactor module is drift we want surfaced, not hidden.
declare -A UPSTREAM       # dir -> space-separated upstream dirs
resolve_edge() {          # $1 = referencing dir, $2 = referenced artifactId; echoes dir or errors
  local from="$1" id="$2"
  if [[ -n "${ARTIFACT_DIR[$id]+x}" ]]; then
    echo "${ARTIFACT_DIR[$id]}"
  elif [[ "$id" == geomesa* ]]; then
    echo "$(basename "$0"): ${from:-<root>} references unknown reactor module '$id'" 1>&2
    exit 1
  fi
}

for ((j = 0; j < ${#DIRS[@]}; j++)); do
  dir="${DIRS[j]}"
  ups=""
  pid="${PARENTIDS[j]}"
  if [[ -n "$pid" ]]; then
    up="$(resolve_edge "$dir" "$pid")"
    [[ -n "$up" ]] && ups+=" $up"
  fi
  for did in ${DEPIDS[j]}; do
    up="$(resolve_edge "$dir" "$did")"
    [[ -n "$up" ]] && ups+=" $up"
  done
  UPSTREAM["$dir"]="$ups"
done

# seed the closure from the -pl list, then BFS over upstream edges. does not account for maven's '!' exclusion syntax,
# which does not ever get passed in here.
declare -A IN_CLOSURE
queue=()
IFS=',' read -ra ENTRIES <<< "$LIST"
for entry in "${ENTRIES[@]}"; do
  # trim surrounding whitespace and any trailing slash
  entry="${entry#"${entry%%[![:space:]]*}"}"
  entry="${entry%"${entry##*[![:space:]]}"}"
  entry="${entry%/}"
  [[ -z "$entry" ]] && continue
  if [[ -z "${IS_DIR[$entry]+x}" ]]; then
    echo "$(basename "$0"): unknown module '$entry' in list" 1>&2
    exit 1
  fi
  if [[ -z "${IN_CLOSURE[$entry]+x}" ]]; then
    IN_CLOSURE["$entry"]=1
    queue+=("$entry")
  fi
done

while ((${#queue[@]})); do
  cur="${queue[0]}"
  queue=("${queue[@]:1}")
  for up in ${UPSTREAM[$cur]}; do
    if [[ -z "${IN_CLOSURE[$up]+x}" ]]; then
      IN_CLOSURE["$up"]=1
      queue+=("$up")
    fi
  done
done

# resolve a file to its owning module: the longest module dir that prefixes the file path
owning_module() {
  local file="$1" owner="" mod
  for mod in "${DIRS[@]}"; do
    if [[ "$mod" == "." ]]; then
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
  if [[ -z "$owner" ]] || [[ -n "${IN_CLOSURE[$owner]+x}" ]]; then
    echo "true"
    exit 0
  fi
done <<< "$CHANGED"

echo "false"
