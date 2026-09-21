#!/usr/bin/env bash

# Reports each job in a completed workflow run with its skip decision and wall-clock duration,
# so a reviewer can validate the affected-module skip logic at a glance: skipped jobs
# should be fast (checkout + cache + detect only), built jobs should take longer. A "skipped" job
# with a long duration - or a "built" job that finished in seconds - points at a bug in the skip
# logic.
#
# The decision is read from the run itself: the build steps (e.g. Set Scala version / Compile) carry
# `if: ... affected != 'false'`, so when a job is skipped the jobs API reports those steps with
# conclusion "skipped", and when it built they are success/failure. A job with an empty project
# list (build-and-test's spark/2.12, where spark modules don't exist for scala 2.12) is reported
# as "empty" rather than "skipped" - it never had anything to build.
#
# It is meant to run as a final job (needs: <matrix job>, if: always()) via workflow_call, reading
# its own run's jobs. The report job itself is still in_progress at that point, so we consider only
# jobs whose status is "completed" - which also drops the report job from its own table.
#
# Output is a markdown table written to the GitHub step summary (or stdout when run locally).
#
# Usage: job-status-report.sh <run-id>
# Env: GH_TOKEN - a token with 'actions: read' on this repo (the workflow's GITHUB_TOKEN is enough)
#      GITHUB_REPOSITORY - owner/repo (set automatically in Actions; pass it when running locally)
#      WORKFLOW_NAME - optional label for the report heading (the completed workflow's name)
# Requires gh and jq

set -eo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $(basename "$0") <run-id>" 1>&2
  exit 1
fi

RUN_ID="$1"

# the run id comes from the workflow_run event payload, not from PR content, but validate anyway so
# a malformed value can't be spliced into the api path below.
if [[ ! "$RUN_ID" =~ ^[0-9]+$ ]]; then
  echo "$(basename "$0"): run id must be numeric" 1>&2
  exit 1
fi

if [[ -z "${GITHUB_REPOSITORY:-}" ]]; then
  echo "$(basename "$0"): GITHUB_REPOSITORY must be set (owner/repo)" 1>&2
  exit 1
fi

# the full table goes to the step summary (rendered markdown); fall back to stdout when run locally.
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/stdout}"

# pull every job for the run, including its steps. --paginate handles runs with more than one page
# of matrix jobs (our build matrix is well over the 30-per-page default). we keep name/status/
# conclusion/timestamps plus the per-step conclusions, which is where the real skip decision lives.
# we keep only completed jobs: the report job that invokes this is itself still in_progress, so
# filtering on status drops it from its own table (and any other not-yet-finished job).
JOBS="$(gh api --paginate "/repos/${GITHUB_REPOSITORY}/actions/runs/${RUN_ID}/jobs" \
  -q '.jobs[] | select(.status == "completed") | {name, status, conclusion, started_at, completed_at, steps: [.steps[] | {name, conclusion}]}' \
  | jq -s '.')"

count="$(jq 'length' <<< "$JOBS")"
if [[ "$count" -eq 0 ]]; then
  echo "$(basename "$0"): no jobs found for run ${RUN_ID}" 1>&2
  exit 1
fi

# classify a job as built / skipped / empty:
#   built   - an affected-gated step actually ran (conclusion success or failure). "Set Scala
#             version" and "Compile" are gated on `affected != 'false'` in every workflow this
#             report covers (build, integration-tests, assembly, spark), so keying on them works
#             uniformly regardless of what each workflow calls its later test/build step.
#   empty   - the job's project list was empty, so it never had anything to build (build-and-test's
#             spark/2.12, where spark modules don't exist for scala 2.12). that case gates even
#             "Detect affected modules" off (on `list != ''`), so it is distinguished from a normal
#             skip by detection having been skipped rather than having run and returned affected=false.
#   skipped - detection ran and determined the PR doesn't touch this job's modules.
job_decision() {  # $1 = job index; echoes "built" | "skipped" | "empty"
  local ran detect
  ran="$(jq -r ".[$1].steps[]
    | select(.name == \"Set Scala version\" or .name == \"Compile\")
    | select(.conclusion == \"success\" or .conclusion == \"failure\")
    | .name" <<< "$JOBS")"
  if [[ -n "$ran" ]]; then echo "built"; return; fi
  detect="$(jq -r ".[$1].steps[]
    | select(.name == \"Detect affected modules\") | .conclusion" <<< "$JOBS")"
  [[ "$detect" == "skipped" ]] && echo "empty" || echo "skipped"
}

# duration in whole seconds between two ISO timestamps; empty if either is missing.
duration_secs() {  # $1 = start, $2 = end; echoes integer seconds or nothing
  [[ -z "$1" || -z "$2" || "$1" == "null" || "$2" == "null" ]] && return
  echo "$(( $(date -d "$2" +%s) - $(date -d "$1" +%s) ))"
}

fmt_duration() {  # $1 = seconds (may be empty); echoes "Nm Ns" / "Ns" / "–"
  [[ -z "$1" ]] && { echo "–"; return; }
  if (( $1 >= 60 )); then
    echo "$(( $1 / 60 ))m $(( $1 % 60 ))s"
  else
    echo "${1}s"
  fi
}

conclusion_icon() {  # $1 = conclusion (or status for in-progress)
  case "$1" in
    success)   echo "✅" ;;
    failure)   echo "❌" ;;
    cancelled) echo "🚫" ;;
    skipped)   echo "⏭️" ;;
    *)         echo "▶️" ;;
  esac
}

# collect one tab-separated record per job, then sort by job name so related jobs (e.g. a matrix
# entry's 2.12 and 2.13 variants) sit next to each other and the table order is stable across runs.
records=""
n_built=0 n_skipped=0 n_empty=0 total_secs=0
for ((i = 0; i < count; i++)); do
  name="$(jq -r ".[$i].name" <<< "$JOBS")"
  status="$(jq -r ".[$i].status" <<< "$JOBS")"
  conclusion="$(jq -r ".[$i].conclusion // \"\"" <<< "$JOBS")"
  started="$(jq -r ".[$i].started_at // \"\"" <<< "$JOBS")"
  completed="$(jq -r ".[$i].completed_at // \"\"" <<< "$JOBS")"

  decision="$(job_decision "$i")"
  secs="$(duration_secs "$started" "$completed")"
  [[ -n "$secs" ]] && total_secs=$(( total_secs + secs ))

  case "$decision" in
    built)   n_built=$(( n_built + 1 ));   decision_cell="🔨 built" ;;
    empty)   n_empty=$(( n_empty + 1 ));   decision_cell="🚧 empty" ;;
    *)       n_skipped=$(( n_skipped + 1 )); decision_cell="⏭️ skipped" ;;
  esac

  mark="$(conclusion_icon "${conclusion:-$status}")"
  records+="${name}\t${decision_cell}\t${mark} ${conclusion:-$status}\t$(fmt_duration "$secs")"$'\n'
done

{
  echo "### Job Status Report${WORKFLOW_NAME:+ - $WORKFLOW_NAME}"
  echo ""
  echo "Job status for run [${RUN_ID}](${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY}/actions/runs/${RUN_ID})"
  echo ""
  echo "| Job | Decision | Status | Duration |"
  echo "| --- | --- | --- | --- |"
  printf '%b' "$records" | sort -t$'\t' -k1,1 \
    | awk -F'\t' 'NF >= 4 { printf "| `%s` | %s | %s | %s |\n", $1, $2, $3, $4 }'
  echo ""
  echo "_${count} jobs, ${n_built} built, ${n_skipped} skipped, ${n_empty} empty, cumulative runtime $(( total_secs / 60 ))m $(( total_secs % 60 ))s._"
} >> "$SUMMARY"
