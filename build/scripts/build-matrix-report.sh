#!/usr/bin/env bash

# Builds a markdown report of which matrix jobs a PR affects, for posting as a single sticky PR
# comment. It reads each workflow's project matrix out of the workflow file, and for each project runs
# detect-affected-modules.sh and records built vs skipped. The report has one section per
# workflow plus the changed files that drove the decisions - so a reviewer can eyeball a
# "skipped" job against the diff and catch a wrong skip.
#
# This only reports; it gates nothing. Output goes to stdout.
#
# Usage: build-matrix-report.sh <workflow.yml> [<workflow.yml> ...]
#   each <workflow.yml> must have exactly one job with a .strategy.matrix.projects list whose
#   entries carry .name and .list (matching what the detect step passes as PROJECT_LIST).
# Env: BASE_SHA, HEAD_SHA - the pull request base/head shas
# Requires yq and jq

set -eo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $(basename "$0") <workflow.yml> [<workflow.yml> ...]" 1>&2
  exit 1
fi

# fetch the two commits once up front: this satisfies every per-project detect-affected-modules.sh
# call below (it re-fetches only if the commits are missing), so we pay one fetch, not one per
# project. we count the diff here to show how many files drove the decisions.
git fetch --no-tags --depth=1 origin "$BASE_SHA" "$HEAD_SHA" 1>&2
CHANGED="$(git diff --name-only "$BASE_SHA" "$HEAD_SHA")"

# emit the built/skipped table for a single workflow file to stdout
report_workflow() {
  local wf="$1" label projects count i name list affected
  label="$(basename "$wf" .yml)"

  # the matrix job's name varies per workflow (build, assembly, integration-tests, ...), so find
  # the one job that has a projects matrix rather than hard-coding a job name. reading it from the
  # workflow means this report and the job it describes can never disagree about the project list.
  projects="$(yq -o=json -I=0 '.jobs[] | select(.strategy.matrix.projects != null) | .strategy.matrix.projects' "$wf")"
  if [[ -z "$projects" || "$projects" == "null" ]]; then
    echo "$(basename "$0"): no projects matrix found in $wf" 1>&2
    exit 1
  fi

  local built_rows="" skipped_rows="" n_built=0 n_skipped=0
  count="$(jq 'length' <<< "$projects")"
  for ((i = 0; i < count; i++)); do
    name="$(jq -r ".[$i].name" <<< "$projects")"
    list="$(jq -r ".[$i].list" <<< "$projects")"
    affected="$(build/scripts/detect-affected-modules.sh "$BASE_SHA" "$HEAD_SHA" "$list")"
    if [[ "$affected" == "true" ]]; then
      built_rows+="| \`$name\` | ✅ built |"$'\n'
      n_built=$((n_built + 1))
    else
      skipped_rows+="| \`$name\` | ⏭️ skipped |"$'\n'
      n_skipped=$((n_skipped + 1))
    fi
  done

  echo "#### \`$label\` — $n_built built, $n_skipped skipped"
  echo ""
  echo "| Job | Decision |"
  echo "| --- | --- |"
  printf '%s' "$built_rows"
  printf '%s' "$skipped_rows"
  echo ""
}

n_changed=0
[[ -n "$CHANGED" ]] && n_changed="$(wc -l <<< "$CHANGED")"

# the marker lets the workflow find and replace this exact comment instead of posting a new one
echo "<!-- build-matrix-report -->"
echo "### Build Matrix Report"
echo ""
for wf in "$@"; do
  report_workflow "$wf"
done
# report only the count, not the filenames: under pull_request_target this comment is posted with
# a write token, and PR-authored filenames are attacker-controlled - echoing them into markdown
# would allow comment injection (e.g. a filename that breaks out of a code fence). the count is
# a plain integer and safe.
echo "_Based on $n_changed changed file(s)._"
