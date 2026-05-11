#!/usr/bin/env bash

# Create or update the Maven JDK toolchains in ~/.m2/toolchains.xml that this project's
# build requires. It establishes the required JDK majors by reading the root <jdk.version>
# (the Maven JVM / main-reactor JDK, e.g. 17) and by scanning EVERY module pom for
# maven-toolchains-plugin requirements. It discovers those JDKs on this machine and wires them into
# toolchains.xml, or tells you which JDK(s) to install if any are missing.
#
# Targets Maven's USER toolchains (~/.m2/toolchains.xml, derived from user.home; override with --toolchains (mirrors `mvn -t`).
#
# Usage:
#   build/scripts/update-maven-toolchains.sh                     # discover + write ~/.m2/toolchains.xml
#   build/scripts/update-maven-toolchains.sh --dry-run           # print what would be written, no changes
#   build/scripts/update-maven-toolchains.sh --toolchains <path> # target a specific file (e.g. mvn -t)



set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ROOT_POM="$ROOT/pom.xml"
DRY_RUN=0
TOOLCHAINS_ARG=""

usage() {
  sed -n '3,16p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --toolchains)
      [ $# -ge 2 ] || { echo "--toolchains requires a path argument" >&2; usage 1; }
      TOOLCHAINS_ARG="$2"; shift ;;
    -h|--help) usage 0 ;;
    *) echo "Unknown argument: $1" >&2; usage 1 ;;
  esac
  shift
done

# Default to Maven's user toolchains (~/.m2/toolchains.xml); --toolchains overrides it.
TOOLCHAINS="${TOOLCHAINS_ARG:-$HOME/.m2/toolchains.xml}"

# --- Determine the major version from a version string ("1.8.0_xxx" -> 8, "25.0.3" -> 25). ---
major_of() {
  local v="$1"
  case "$v" in
    1.*) v="${v#1.}"; echo "${v%%.*}" ;;
    *)   echo "${v%%.*}" ;;
  esac
}

# --- Does $1 (a JDK home) contain a JDK whose major version == $2? ---
is_jdk_major() {
  local home="$1" want="$2" ver=""
  [ -n "$home" ] && [ -x "$home/bin/javac" ] || return 1
  if [ -r "$home/release" ]; then
    ver="$(sed -n 's/^JAVA_VERSION="\([^"]*\)".*/\1/p' "$home/release" | head -1)"
  fi
  [ -n "$ver" ] || ver="$("$home/bin/javac" -version 2>&1 | awk '{print $2}')"
  [ "$(major_of "$ver")" = "$want" ]
}

# --- Find a JDK home for major version $1; echoes the canonical path on success. ---
discover_jdk_home() {
  local want="$1"
  local cand home n
  for n in "JAVA${want}_HOME" "JAVA_HOME_${want}_X64" "JAVA_HOME_${want}_ARM64" "JAVA_HOME_${want}_AARCH64" JAVA_HOME; do
    cand="${!n:-}"
    if is_jdk_major "$cand" "$want"; then ( cd "$cand" && pwd -P ); return 0; fi
  done
  if command -v /usr/libexec/java_home >/dev/null 2>&1; then   # macOS
    home="$(/usr/libexec/java_home -v "$want" 2>/dev/null || true)"
    if is_jdk_major "$home" "$want"; then ( cd "$home" && pwd -P ); return 0; fi
  fi
  for cand in \
      /usr/lib/jvm/* /usr/java/* /opt/java/* \
      /opt/hostedtoolcache/Java_*/"${want}".*/x64 /opt/hostedtoolcache/Java_*/"${want}".*/*/Contents/Home \
      /Library/Java/JavaVirtualMachines/*/Contents/Home \
      "$HOME"/.sdkman/candidates/java/* \
      /opt/homebrew/opt/openjdk@"$want" /usr/local/opt/openjdk@"$want" \
      /opt/homebrew/opt/openjdk@"$want"/libexec/openjdk.jdk/Contents/Home; do
    [ -d "$cand" ] || continue
    if is_jdk_major "$cand" "$want"; then ( cd "$cand" && pwd -P ); return 0; fi
  done
  return 1
}

# --- Per-OS install hint for a missing JDK major version. ---
install_hint() {
  local v="$1"
  echo "  • JDK $v:"
  case "$(uname -s)" in
    Darwin)
      echo "      brew install openjdk@$v"
      echo "      # then symlink it so the JVM is discoverable, e.g.:"
      echo "      sudo ln -sfn \"\$(brew --prefix)/opt/openjdk@$v/libexec/openjdk.jdk\" /Library/Java/JavaVirtualMachines/openjdk-$v.jdk"
      ;;
    Linux)
      command -v apt-get >/dev/null 2>&1 && echo "      sudo apt-get install -y openjdk-$v-jdk"
      command -v dnf     >/dev/null 2>&1 && echo "      sudo dnf install -y java-$v-openjdk-devel"
      ;;
  esac
  echo "      # any platform (SDKMAN — https://sdkman.io): sdk install java $v-open"
  echo "      # or set JAVA${v}_HOME to an existing JDK $v and re-run this script."
}

# --- Introspect the required JDK majors from the poms (fall back to known defaults). ---
read_prop() {  # read_prop <file> <tagname> <default>
  local val=""
  [ -r "$1" ] && val="$(grep -oE "<$2>[0-9]+" "$1" | head -1 | grep -oE '[0-9]+' || true)"
  echo "${val:-$3}"
}
# --- Resolve ${property} references against <property>value</property> in any pom (a few levels). ---
resolve_value() {
  local val="$1" depth=0 name esc pv
  while printf '%s' "$val" | grep -q '[$]{'; do
    depth=$((depth + 1)); [ "$depth" -gt 6 ] && break
    name="$(printf '%s' "$val" | sed -n 's/.*[$]{\([^}]*\)}.*/\1/p')"
    [ -n "$name" ] || break
    esc="$(printf '%s' "$name" | sed 's/[.]/\\./g')"
    pv="$(grep -rhoE "<$esc>[^<]+</$esc>" --include=pom.xml "$ROOT" 2>/dev/null | head -1 | sed "s|<$esc>||; s|</$esc>||")"
    [ -n "$pv" ] || break
    val="$(printf '%s' "$val" | sed "s|[$]{$name}|$pv|")"
  done
  printf '%s' "$val"
}

# --- Scan every module pom for maven-toolchains-plugin JDK requirements. Emits "<major>\t<relpath>"
#     for each <jdk><version>…</version></jdk> (profile <activation><jdk>[N,)</jdk></activation> has
#     no nested <version>, so activation ranges are naturally ignored).
scan_toolchain_reqs() {
  local pom raw rv
  while IFS= read -r -d '' pom; do
    awk '
      /<jdk>/ { injdk = 1 }
      injdk && match($0, /<version>[^<]+<\/version>/) {
        v = $0; sub(/.*<version>[[:space:]]*/, "", v); sub(/[[:space:]]*<\/version>.*/, "", v); print v
      }
      /<\/jdk>/ { injdk = 0 }
    ' "$pom" | while IFS= read -r raw; do
      rv="$(resolve_value "$raw")"
      [ -n "$rv" ] && printf '%s\t%s\n' "$(major_of "$rv")" "${pom#"$ROOT"/}"
    done
  done < <(find "$ROOT" -name pom.xml -not -path '*/target/*' -print0)
}

BASE_JDK="$(read_prop "$ROOT_POM" 'jdk.version' 17)"
TOOLCHAIN_REQS="$(scan_toolchain_reqs)"
TOOLCHAIN_VERSIONS="$(printf '%s\n' "$TOOLCHAIN_REQS" | awk -F'\t' 'NF{print $1}' | sort -un)"
REQUIRED="$(printf '%s\n%s\n' "$BASE_JDK" "$TOOLCHAIN_VERSIONS" | grep -E '^[0-9]+$' | sort -un)"

echo "GeoMesa build JDK requirements:"
echo "  • JDK $BASE_JDK  — the Maven JVM / main reactor (root <jdk.version>; Scala 2.12, Arrow)"
if [ -n "$TOOLCHAIN_VERSIONS" ]; then
  for v in $TOOLCHAIN_VERSIONS; do
    mods="$(printf '%s\n' "$TOOLCHAIN_REQS" | awk -F'\t' -v want="$v" '$1==want{print $2}' | sort -u | paste -sd', ' -)"
    echo "  • JDK $v  — maven-toolchains-plugin requirement, forked at compile time (needed by: $mods)"
  done
else
  echo "  (no maven-toolchains-plugin requirements found in any module pom)"
fi
echo

# --- Discover each required JDK. ---
FOUND=""      # lines of "<major>\t<home>"
MISSING=""    # space-separated majors
for v in $REQUIRED; do
  if home="$(discover_jdk_home "$v")"; then
    printf '  found JDK %s -> %s\n' "$v" "$home"
    FOUND="${FOUND}${v}	${home}
"
  else
    printf '  MISSING JDK %s\n' "$v"
    MISSING="${MISSING} ${v}"
  fi
done
MISSING="${MISSING# }"
echo

if [ -n "$MISSING" ]; then
  echo "Cannot configure toolchains — these required JDK(s) were not found:"
  for v in $MISSING; do install_hint "$v"; done
  echo
  echo "Install the JDK(s) above (or set JAVA<major>_HOME), then re-run:"
  echo "  build/scripts/update-maven-toolchains.sh"
  exit 1
fi

# --- Render the managed <toolchain> blocks for the versions found
MANAGED_XML="$(mktemp)"
trap 'rm -f "$MANAGED_XML"' EXIT
printf '%s' "$FOUND" | while IFS='	' read -r v home; do
  [ -n "$v" ] || continue
  cat >> "$MANAGED_XML" <<EOF
  <toolchain>
    <type>jdk</type>
    <provides>
      <version>$v</version>
    </provides>
    <configuration>
      <jdkHome>$home</jdkHome>
    </configuration>
  </toolchain>
EOF
done

# Compose the new toolchains.xml: preserve existing entries entries, add/update managed jdk entries.
RENDERED="$(mktemp)"
trap 'rm -f "$MANAGED_XML" "$RENDERED"' EXIT
MANAGED_VERSIONS="$(echo "$REQUIRED" | tr '\n' ' ')"

if [ -f "$TOOLCHAINS" ] && grep -q '</toolchains>' "$TOOLCHAINS"; then
  # Drop existing jdk toolchains whose major version we manage, keep everything else,
  # and insert the freshly-rendered managed blocks just before </toolchains>.
  awk -v managed="$MANAGED_VERSIONS" -v insert="$MANAGED_XML" '
    function major(v,  m) { if (v ~ /^1\./) { m=v; sub(/^1\./,"",m); sub(/\..*/,"",m); return m }
                            m=v; sub(/\..*/,"",m); return m }
    BEGIN { n=split(managed, a, " "); for (i=1;i<=n;i++) want[a[i]]=1 }
    /<toolchain>/ { inblk=1; buf=""; isjdk=0; ver="" }
    inblk {
      buf = buf $0 "\n"
      if ($0 ~ /<type>[[:space:]]*jdk[[:space:]]*<\/type>/) isjdk=1
      if (match($0, /<version>[[:space:]]*[0-9._]+[[:space:]]*<\/version>/)) {
        ver=$0; sub(/.*<version>[[:space:]]*/,"",ver); sub(/[[:space:]]*<\/version>.*/,"",ver)
      }
      if ($0 ~ /<\/toolchain>/) {
        inblk=0
        if (!(isjdk && (major(ver) in want))) printf "%s", buf
      }
      next
    }
    /<\/toolchains>/ {
      while ((getline line < insert) > 0) print line
      close(insert)
      print; next
    }
    { print }
  ' "$TOOLCHAINS" > "$RENDERED"
else
  # No existing file — create new toolchains.xml
  {
    echo '<?xml version="1.0" encoding="UTF-8"?>'
    echo '<toolchains xmlns="http://maven.apache.org/TOOLCHAINS/1.1.0"'
    echo '            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
    echo '            xsi:schemaLocation="http://maven.apache.org/TOOLCHAINS/1.1.0 http://maven.apache.org/xsd/toolchains-1.1.0.xsd">'
    cat "$MANAGED_XML"
    echo '</toolchains>'
  } > "$RENDERED"
fi

if [ "$DRY_RUN" -eq 1 ]; then
  echo "--- $TOOLCHAINS (dry-run, not written) ---"
  cat "$RENDERED"
  exit 0
fi

# No-op if the rendered content matches what's already there — avoids a needless rewrite + backup.
if [ -f "$TOOLCHAINS" ] && cmp -s "$RENDERED" "$TOOLCHAINS"; then
  echo "$TOOLCHAINS already up to date (JDK toolchains:$( for v in $REQUIRED; do printf ' %s' "$v"; done ) ) — left unchanged"
  exit 0
fi

mkdir -p "$(dirname "$TOOLCHAINS")"
# Only back up when we're actually changing existing content.
if [ -f "$TOOLCHAINS" ]; then
  backup="$TOOLCHAINS.bak.$(date +%Y%m%d%H%M%S)"
  cp "$TOOLCHAINS" "$backup"
  echo "Backed up existing toolchains -> $backup"
fi
cp "$RENDERED" "$TOOLCHAINS"

echo "Wrote $TOOLCHAINS with JDK toolchains:$( for v in $REQUIRED; do printf ' %s' "$v"; done )"
echo
forked="$(printf '%s' "$TOOLCHAIN_VERSIONS" | paste -sd', ' -)"
echo "Build the full geomesa artifact suite: (Maven JVM on JDK $BASE_JDK; forked toolchain JDK(s): ${forked:-none}):"
base_home="$(printf '%s' "$FOUND" | awk -F'\t' -v v="$BASE_JDK" '$1==v{print $2; exit}')"
echo "  JAVA_HOME=\"$base_home\" mvn clean install"
