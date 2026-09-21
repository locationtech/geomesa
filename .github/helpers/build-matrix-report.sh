#!/usr/bin/env bash

# Reports which matrix jobs a PR affects vs skips, across the build workflows. It reads each
# workflow's project matrix out of the workflow file, and for each project runs
# detect-affected-modules.sh to record built vs skipped.
#
# Output is two things, both from this single job (not one annotation per skipped job):
#   - a markdown table written to the GitHub step summary (or stdout when run locally), one
#     section per workflow, so a reviewer can see the full built/skipped picture
#   - a "::warning::" annotation for each workflow that skipped any job, naming the skipped jobs,
#     so an incorrect skip is easy to spot from the PR's checks
#
# This only reports; it gates nothing.
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

# the full table goes to the step summary (rendered markdown, no token needed - works for forks);
# fall back to stdout when run locally. "::warning::" annotations always go to stdout so the
# GitHub runner picks them up.
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/stdout}"

# fetch the two commits once up front: this satisfies every per-project detect-affected-modules.sh
# call below (it re-fetches only if the commits are missing), so we pay one fetch, not one per
# project.
git fetch --no-tags --depth=1 origin "$BASE_SHA" "$HEAD_SHA" 1>&2

# read a workflow's project matrix as compact json. the matrix job's name varies per workflow
# (build, assembly, integration-tests, ...), so find the one job that has a projects matrix rather
# than hard-coding a job name. reading it from the workflow means this report and the job it
# describes can never disagree about the project list.
workflow_projects() {
  local wf="$1" projects
  projects="$(yq -o=json -I=0 '.jobs[] | select(.strategy.matrix.projects != null) | .strategy.matrix.projects' "$wf")"
  if [[ -z "$projects" || "$projects" == "null" ]]; then
    echo "$(basename "$0"): no projects matrix found in $wf" 1>&2
    exit 1
  fi
  printf '%s' "$projects"
}

# pass 1: collect every matrix project's list across all workflows, in order. we evaluate them all
# in a single detect-affected-modules.sh --batch call below, which builds the pom dependency graph
# and diffs the commits once and reuses that for every list - instead of paying for it once per
# list (~40x for our matrices).
WF_LABELS=()             # per-workflow label ("build-and-test", ...), one per workflow arg
WF_PROJECTS=()           # per-workflow projects json, index-aligned with WF_LABELS
ALL_LISTS=()             # every project's list, flattened in workflow-then-index order
for wf in "$@"; do
  projects="$(workflow_projects "$wf")"
  WF_LABELS+=("$(basename "$wf" .yml)")
  WF_PROJECTS+=("$projects")
  count="$(jq 'length' <<< "$projects")"
  for ((i = 0; i < count; i++)); do
    # spark modules only build for scala 2.13 and live in a separate list-2.13 element; join it
    # with the main list so both are reported (an entry may have only one of the two)
    ALL_LISTS+=("$(jq -r ".[$i] | [.list, .[\"list-2.13\"]] | map(select(. != null)) | join(\",\")" <<< "$projects")")
  done
done

# one batch call: one "true"/"false" per list, in the same order we fed them in.
mapfile -t DECISIONS < <(printf '%s\n' "${ALL_LISTS[@]}" | .github/helpers/detect-affected-modules.sh --batch "$BASE_SHA" "$HEAD_SHA")
if [[ "${#DECISIONS[@]}" -ne "${#ALL_LISTS[@]}" ]]; then
  echo "$(basename "$0"): expected ${#ALL_LISTS[@]} decisions from detect, got ${#DECISIONS[@]}" 1>&2
  exit 1
fi

{
  echo "### Build Matrix Report"
  echo ""
  echo "Which matrix jobs this PR builds vs skips, based on which files are changed in the PR."
  echo ""
} >> "$SUMMARY"

# pass 2: render one section per workflow from the precomputed decisions, and emit a warning for
# any workflow that skipped jobs.
d=0   # running index into DECISIONS, consumed in the same order lists were collected
for w in "${!WF_PROJECTS[@]}"; do
  label="${WF_LABELS[w]}"
  projects="${WF_PROJECTS[w]}"
  count="$(jq 'length' <<< "$projects")"

  built_rows="" skipped_rows="" skipped_names="" n_built=0 n_skipped=0
  for ((i = 0; i < count; i++)); do
    name="$(jq -r ".[$i].name" <<< "$projects")"
    if [[ "${DECISIONS[d]}" == "true" ]]; then
      built_rows+="| \`$name\` | ✅ built |"$'\n'
      n_built=$((n_built + 1))
    else
      skipped_rows+="| \`$name\` | ⏭️ skipped |"$'\n'
      skipped_names+="${skipped_names:+, }$name"
      n_skipped=$((n_skipped + 1))
    fi
    d=$((d + 1))
  done

  {
    echo "#### \`$label\` — $n_built built, $n_skipped skipped"
    echo ""
    echo "| Job | Decision |"
    echo "| --- | --- |"
    printf '%s' "$built_rows"
    printf '%s' "$skipped_rows"
    echo ""
  } >> "$SUMMARY"

  # names come from the workflow's own .name fields (not from PR content), so they are safe to put
  # in an annotation. keep it single-line - "::warning::" does not render raw newlines.
  if (( n_skipped > 0 )); then
    echo "::warning title=Build matrix report::${label}: skipped ${n_skipped} of ${count} matrix jobs (${skipped_names})."
  fi
done
