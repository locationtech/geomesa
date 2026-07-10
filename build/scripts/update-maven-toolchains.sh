#!/usr/bin/env bash
# requirements: bash (4.2 or later), coreutils (basename date dirname env mktemp readlink tr uname wc),
#               diffutils (cmp), findutils (find), gawk (or equivalent), grep, sed
#               (all packages are widely available GNU software)

[[ "$0" == -* ]] && PROG='update-maven-toolchains' || PROG="$0"
HERE=$(readlink -f .)
SELF=$(basename `readlink -f "$PROG"`)

# 1: error message
# 2: (optional) exit status, default is 1
# print message to stderr and exit
function err_quit {
  echo >&2 "$1"
  exit ${2:-1}
}

# the -v test needs at-least 4.2 and is very useful for indirect-expansion checks
[[ -z "$BASH" ]] && err_quit 'must be run by a bash-compatible shell'
if [[ ${BASH_VERSINFO[0]} -lt 4 ]]; then
  err_quit "must be run by bash 4.2 or later, found: $BASH_VERSION"
elif [[ ${BASH_VERSINFO[0]} -eq 4 && ${BASH_VERSINFO[1]} -lt 2 ]]; then
  err_quit "must be run by bash 4.2 or later, found: $BASH_VERSION"
fi

# prints help to stderr and exits 1
function help_quit {
  echo >&2 
  echo >&2 "Scans project POMs for Maven toolchain JDKs, and creates a toolchains.xml file that can be used"
  echo >&2 "to build those projects. The JDKs required by project POMs will need to be installed so this"
  echo >&2 "tool knows which paths to place in toolchains.xml, and if some JDK is missing then a suggestion"
  echo >&2 "will be provided."
  echo >&2
  echo >&2 "Usage: $SELF [-h] [-d] [-b PATH] [-o PATH]"
  echo >&2
  echo >&2 "Options:"
  echo >&2 "  -h       show this help and exit"
  echo >&2 "  -d       dry-run, write to stdout instead of a file"
  echo >&2 "  -b PATH  project base directory (default is cwd)"
  echo >&2 "  -o PATH  write configuration here (default: ~/.m2/toolchains.xml)"
  echo >&2
  echo >&2 "Examples:"
  echo >&2 "  $SELF"
  echo >&2 "  $SELF -o /path/to/toolchains.xml"
  echo >&2 "  $SELF -d -b /path/to/project"
  echo >&2
  exit 1
}

# 1: target to search for
# *: array elements
# return 0 (success) if target is one of the elements
# return 1 (failure) if target is None of the elements
# return 2 (failure) if target is missing/empty
# return 3 (failure) if elements are missing/empty
function array_contains {
	local target="$1"
	[[ -n "$target" ]] || return 2
	shift || return 3
	for elem in "${@}"; do
		[[ "$elem" == "$target" ]] && return 0
	done
	return 1
}

# 1: version
# if version is "1.x.etc" then MAJOR_VERSION is x (since 1.8 is "java 8")
# otherwise, for "y.z.etc", then MAJOR_VERSION is y (since 27.3 is "java 27")
function major_version {
  local version="$1"
  [[ "$version" =~ /^1\./ ]] && version="${version#1.}"
  MAJOR_VERSION="${version%%.*}"
}

# 1: directory of JDK to inspect
# 2: desired JDK version (ie. 7, 25, etc)
# return 0 (success) if dir contains JDK with specified major version
# otherwise return 1 (failure)
function jdk_present {
  local jdk_dir="$1"
  local desired="$2" actual
  [[ -z "$desired" ]] && return 1
  [[ -z "$jdk_dir" ]] && return 1
  [[ -d "$jdk_dir" ]] || return 1
  [[ -x "${jdk_dir}/bin/javac" ]] || return 1
  [[ -r "${jdk_dir}/release" ]] && actual="$(sed -n 's#^JAVA_VERSION="\([^"]\+\)".*#\1#p' "${jdk_dir}/release" | head -1)"
  [[ -z "$actual" ]] && actual=$("${jdk_dir}/bin/javac" -version |& head -1 | sed -e 's#^javac \([^ ]\+\).*$#\1#')
  major_version "$actual"
  [[ "$MAJOR_VERSION" = "$desired" ]] && return 0
  return 1
}

# 1: desired JDK version (eg. 25)
# DISCOVERED_JDK is set to first directory that contains JDK of specified version
# return 0 (success) if a JDK was found, otherwise return 1 (failure)
function discover_jdk_home {
  local -a guesses
  local -a candidates
  local candidate
  local desired="$1"
  shift
  DISCOVERED_JDK=''
  candidates=("JAVA${desired}_HOME" "JAVA_HOME_${desired}_X64")
  candidates+=("JAVA_HOME_${desired}_ARM64" "JAVA_HOME_${desired}_AARCH64")
  candidates+=("JAVA_HOME")
  # environment variable candidates
  for candidate in "${candidates[@]}"; do
    [[ -v "$candidate" ]] || continue
    candidate="${!candidate}"
    if jdk_present "$candidate" "$desired"; then
      DISCOVERED_JDK=`readlink -f "$candidate"`
      return 0
    fi
  done
  # MacOS tools
  if command -v /usr/libexec/java_home &>/dev/null; then
    candidate=$(/usr/libexec/java_home -v "$desired" 2>/dev/null)
    if [[ -n "$candidate" ]] && jdk_present "$candidate" "$desired"; then
      DISCOVERED_JDK=`readlink -f "$candidate"`
      return 0
    fi
  fi
  # guess some common paths
  guesses=(/usr/lib/jvm /usr/java /opt/java)
  guesses+=(/opt/hostedtoolcache /Library/Java/JavaVirtualMachines)
  guesses+=(~/.sdkman/candidates/java /opt/homebrew/opt)
  guesses+=(/usr/local/opt)
  while read -r candidate; do
    [[ -f "$candidate" ]] || continue
    candidate="$(dirname `dirname "$candidate"`)"
    if [[ -n "$candidate" ]] && jdk_present "$candidate" "$desired"; then
      DISCOVERED_JDK=$(readlink -f "$candidate")
      return 0
    fi
  done < <(find -L "${guesses[@]}" -type f -name javac 2>/dev/null)
  # no joy
  return 1
}

# similar to usage info, except suggests how to install JDKs
function install_hint {
  local version="$1"
  local platform="$(uname -s)"
  echo "  - JDK ${version}"
  case "$platform" in
  Darwin)
    echo "    [Darwin]"
    echo "      brew install openjdk@${version}"
    echo "      # then symlink it so the JVM is discoverable, eg:"
    echo "      sudo ln -sfn \"\$(brew --prefix)/opt/openjdk@${version}/libexec/openjdk.jdk\" /Library/Java/JavaVirtualMachines/openjdk-${version}.jdk"
    ;;
  Linux)
    echo "    [Linux]"
    command -v pacman  >/dev/null 2>&1 && echo "      sudo pacman -S extra/jdk${version}-openjdk --noconfirm"
    command -v apt-get >/dev/null 2>&1 && echo "      sudo apt-get install -y openjdk-${version}-jdk"
    command -v dnf     >/dev/null 2>&1 && echo "      sudo dnf install -y java-${version}-openjdk-devel"
    ;;
  esac
  echo "    [Any]"
  echo "      # SDKMAN (https://sdkman.io): sdk install java ${version}-open"
  echo "      # or override by setting JAVA${version}_HOME to your JDK ${version} location and re-run"
}

# 1: raw xml string to search
# 2: tag to search for
# 3: awk "IGNORECASE" flag, default 0 (case-sensitive)
# finds <tag>...</tag> in xml and assigns matches to FOUND_DATA array
# returns 0 if at-least one match was found, otherwise returns 1
function tag_data_from_string {
  local xml="$1"
  local tag="$2"
  local ignore="${3:-0}"
  local endline
  local found value
  local awk_tag_find
  local sed_open_remove
  local sed_close_remove
  FOUND_DATA=()
  [[ -z "$xml" ]] && return 2
  [[ -z "$tag" ]] && return 3
  [[ $ignore != '0' && $ignore != '1' ]] && return 4
  # L = line counter, F = "found" flag, S = start line
  awk_tag_find="BEGIN{IGNORECASE=${ignore}} {L=L+1} /<\\s*${tag}[^>]*>/{F=1;S=L} /<\s*\\/${tag}\s*>/{F=0;print S,L}"
  sed_open_remove="s#.*<\\s*${tag}[^>]*>##"
  sed_close_remove="s#<\\s*/\\s*${tag}\\s*>.*##"
  while read -r startline endline; do
    found=$(echo -ne "$xml" | sed -n "${startline},${endline}p")
    value=$(echo -ne "$found" | sed -e "$sed_open_remove" -e "$sed_close_remove")
    FOUND_DATA+=("$value")
  done < <(echo -ne "$xml" | awk "$awk_tag_find")
  [[ "${#FOUND_DATA[@]}" -gt 0 ]]
}

# 1: xml file to search
# 2: tag to search for
# 3: awk "IGNORECASE" flag, default 0 (case-sensitive)
# finds <tag>...</tag> in file and assigns matches to FOUND_DATA array
# returns 0 if at-least one match was found, otherwise returns 1
function tag_data_from_file {
  local file="$1"
  local tag="$2"
  local ignore="${3:-0}"
  tag_data_from_string "$(cat "$file")" "$tag" "$ignore"
  # returns status from tag_data_from_string
}

# 1: base dir to find pom.xml files under
# 2: tag to search for
# 3: awk "IGNORECASE" flag, default 0 (case-sensitive)
# finds <tag>value</tag> in pom.xml files under dir
# sets FIRST_VALUE to value and immedaitely returns
# returns 0 if match was found, otherwise returns 1
function first_value_from_poms {
  local dir="$1"
  local tag="$2"
  local ignore="${3:-0}"
  local found
  while read -r pom; do
    tag_data_from_file "$pom" "$tag" "$ignore" || continue
    for found in "${FOUND_DATA[@]}"; do
      FIRST_VALUE="$found"
      return 0
    done
  done < <(find "$dir" -type f -name pom.xml -not -path '*/target/*')
  return 1
}

# 1: variable name to lookup
# 2: base dir to find pom.xml files under
# scans poms for <variable>value</variable> and sets LOOKUP to first found value
# returns 0 if match was found, otherwise returns 1
function lookup_placeholder {
  local varname="$1"
  local dir="$2"
  local trimmed=$(echo -n "$varname" | tr -d '[:space:]')
  local normalized
  LOOKUP=''
  if [[ "$trimmed" == \$\{* && "$trimmed" == *\} ]]; then
    normalized=$(echo -n "$trimmed" | sed -e 's#^${##' -e 's#}$##')
    first_value_from_poms "$dir" "$normalized" || return 1
    LOOKUP="$FIRST_VALUE"
  else
    LOOKUP="$varname"
  fi
}

# 1: base dir to find pom.xml files under
# scans all pom.xml files under dir and examines <toochains> for JDKs
# all defined JDK versions are added to TOOLCHAIN_JDKS array
declare -A TOOLCHAIN_JDKS
function toolchain_jdks {
  local dir="$1"
  local -a toolchain_xmls
  local -a jdk_xmls
  local toolchain_xml
  local jdk_xml
  local found
  local pom_dir
  UNIQUE_JDKS=()
  TOOLCHAIN_JDKS=()
  while read -r pom; do
    pom_dir=$(dirname "$pom")
    tag_data_from_file "$pom" toolchains
    toolchain_xmls=("${FOUND_DATA[@]}")
    for toolchain_xml in "${toolchain_xmls[@]}"; do
      tag_data_from_string "$toolchain_xml" jdk || continue
      jdk_xmls=("${FOUND_DATA[@]}")
      for jdk_xml in "${jdk_xmls[@]}"; do
        tag_data_from_string "$jdk_xml" version || continue
        for found in "${FOUND_DATA[@]}"; do
          lookup_placeholder "$found" "$1"
          array_contains "$LOOKUP" "${UNIQUE_JDKS[@]}" || UNIQUE_JDKS+=("$LOOKUP")
          TOOLCHAIN_JDKS["$pom_dir"]="$LOOKUP"
        done
      done
    done
  done < <(find "$dir" -type f -name pom.xml -not -path '*/target/*')
}

# 1: string of base JDK version
# 2: string of toolchain JDKs versions
function print_build_suggestion {
  local base_jdk="$1"
  local base_jdk_home="$2"
  local toolchain_jdks="$3"
  local msg_prefix
  msg_prefix="Build the full artifact suite using Maven with JDK $base_jdk"
  if [[ -n "$toolchain_jdks" ]]; then
    echo "${msg_prefix} and toolchain JDK(s): ${toolchain_jdks}"
  else
    echo "${msg_prefix} (no toolchain JDKs needed)"
  fi
  echo "  JAVA_HOME=\"${base_jdk_home}\" mvn clean install"
}

# parse options
DRYRUN=0
BASEDIR="$HERE"
OUTPUT=~/.m2/toolchains.xml
while getopts ':hdb:o:' OPT; do
  case "$OPT" in
  h) help_quit;;
  d) DRYRUN=1;;
  b) BASEDIR="$OPTARG";;
  o) OUTPUT="$OPTARG";;
  \?) err_quit "unsupported option: $OPTARG (see -h for help)";;
  *) err_quit "option is missing a value: $OPTARTG (see -h for help)";;
  esac
done
shift $((OPTIND-1))

# find baseline JDK for non-toolchain projects
first_value_from_poms "$BASEDIR" 'jdk.version' || err_quit 'jdk.version property not found'
BASE_JDK="$FIRST_VALUE"

# find all toolchain projects and their JDKs
toolchain_jdks "$BASEDIR"

# quick requirements report
echo
echo "Maven build JDK requirements:"
echo "  - JDK ${BASE_JDK}: baseline JDK used by main reactor"
for PROJ in "${!TOOLCHAIN_JDKS[@]}"; do
  echo "  - JDK ${TOOLCHAIN_JDKS[$PROJ]}: maven-toolchains-plugin for: $PROJ"
done
[[ "${#UNIQUE_JDKS[@]}" -lt 1 ]] && echo "  (no maven-toolchains-plugin requirements found in any pom.xml"
echo

# ensure baseline JDK is installed
MISSING_JDKS=()
if discover_jdk_home "$BASE_JDK"; then
  echo "Found JDK $BASE_JDK at: $DISCOVERED_JDK"
  BASE_JDK_HOME="$DISCOVERED_JDK"
else
  MISSING_JDKS+=("$BASE_JDK")
fi

# ensure all toolchain JDKs are installed
declare -A FOUND_JDKS
for JDK in "${UNIQUE_JDKS[@]}"; do
  if discover_jdk_home "$JDK"; then
    echo "Found JDK $JDK at: $DISCOVERED_JDK"
    FOUND_JDKS["$JDK"]="$DISCOVERED_JDK"
  else
    MISSING_JDKS+=("$JDK")
  fi
done
if [[ "${#MISSING_JDKS[@]}" -gt 0 ]]; then
  echo 'One or more required JDKs could not be found:'
  for MISSING_JDK in "${MISSING_JDKS[@]}"; do
    install_hint "$MISSING_JDK"
  done
  echo
  err_quit 'cannot proceed: install missing JDKs or manually export before running script, eg: JAVA22_HOME=/path/to/jdk22'
fi

# render <toolchain> blocks just for toolchain JDKs
TEMP_JDK_XML="$(mktemp)"
trap 'rm -f "$TEMP_JDK_XML"' EXIT
for JDK in "${!FOUND_JDKS[@]}"; do
  cat >>"$TEMP_JDK_XML" <<EOF
  <toolchain>
    <type>jdk</type>
    <provides>
      <version>${JDK}</version>
    </provides>
    <configuration>
      <jdkHome>${FOUND_JDKS[$JDK]}</jdkHome>
    </configuration>
  </toolchain>
EOF
done

# merge (if needed) toolchain snippets with output file (preserving existing entries entries)
TEMP_OUTPUT_FILE="$(mktemp)"
trap 'rm -f "$TEMP_OUTPUT_FILE" "$TEMP_JDK_XML"' EXIT
if [[ -f "$OUTPUT" ]] && grep -q '</toolchains>' "$OUTPUT"; then
  # drop all toolchains whose major version is managed by this script.
  # insert the toolchain snippets from above just before </toolchains>.
  awk -v "managed=${!FOUND_JDKS[*]}" -v our_jdk_xml_filename="$TEMP_JDK_XML" '
    function major(v, m) {
      m=v;
      if (v ~ /^1\./) {
        sub(/^1\./,"",m);
      }
      sub(/\..*/,"",m);
      return m;
    }
    
    BEGIN {
      n=split(managed, a, " ");
      for (i=1; i<=n; i++) {
        our_jdks[a[i]]=1;
      }
    }
    
    /<toolchain>/ {
      inblk=1;
      isjdk=0;
      buf="";
      ver="";
    }
    
    inblk {
      buf = buf $0 "\n"
      if ($0 ~ /<type>[[:space:]]*jdk[[:space:]]*<\/type>/) {
        isjdk=1;
      }
      if ($0 ~ /<version>[[:space:]]*[0-9._]+[[:space:]]*<\/version>/) {
        ver=$0;
        sub(/.*<version>[[:space:]]*/,"",ver);
        sub(/[[:space:]]*<\/version>.*/,"",ver);
      } else if ($0 ~ /<version>[ 0-9._\[\(\)\],]+<\/version>/) {
        bad_tag=$0;
        sub(/.*<version>/,"<version>",bad_tag);
        sub(/<\/version>.*/,"</version>",bad_tag);
        print "error: cannot manage existing xml with version-ranges: " bad_tag >"/dev/stderr";
        exit 2;
      }
      if ($0 ~ /<\/toolchain>/) {
        inblk=0;
        if (!(isjdk && (major(ver) in our_jdks))) {
          printf "%s", buf;
        }
      }
      next;
    }
    
    /<\/toolchains>/ {
      while ((getline line <our_jdk_xml_filename) > 0) {
        print line;
      }
      close(insert);
      print;
      next;
    }
    
    { print }
  ' "$OUTPUT" >"$TEMP_OUTPUT_FILE" || err_quit 'failed to merge toolchains into existing file'
else
  # no existing file, create new output file
  {
    echo '<?xml version="1.0" encoding="UTF-8"?>'
    echo '<toolchains xmlns="http://maven.apache.org/TOOLCHAINS/1.1.0"'
    echo '            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
    echo '            xsi:schemaLocation="http://maven.apache.org/TOOLCHAINS/1.1.0 http://maven.apache.org/xsd/toolchains-1.1.0.xsd">'
    cat "$TEMP_JDK_XML"
    echo '</toolchains>'
  } >"$TEMP_OUTPUT_FILE"
fi

# respect dry-run flag
if [[ "$DRYRUN" -eq 1 ]]; then
  echo
  echo "dry-run output, would have written to: $OUTPUT"
  echo
  cat "$TEMP_OUTPUT_FILE"
  echo
  exit 0
fi

# shortcut if generated XML matches existing XML
if cmp -s "$TEMP_OUTPUT_FILE" "$OUTPUT"; then
  if [[ "${#UNIQUE_JDKS[@]}" -eq 0 ]]; then
    echo "$OUTPUT already up to date (no toolchain JDKs needed)"
  else
    echo "$OUTPUT already up to date (proper JDK toolchains ${UNIQUE_JDKS[*]} already present)"
  fi
  echo
  print_build_suggestion "$BASE_JDK" "$BASE_JDK_HOME" "${UNIQUE_JDKS[*]}"
  exit 0
fi

# ensure output directory exists
OUTDIR=$(dirname "$OUTPUT")
if [[ ! -d "$OUTDIR" ]]; then
  mkdir -p "$(dirname "$OUTPUT")" || err_quit "failed to create output directory: $OUTDIR"
fi

# backup existing file (if present)
if [[ -f "$OUTPUT" ]]; then
  BACKUP="${OUTPUT}.bak.$(date +%Y%m%d_%H%M%S)"
  cp "$OUTPUT" "$BACKUP" || err_quit "failed to backup '$OUTPUT' to '$BACKUP'"
  echo "created backup file at: $BACKUP"
fi
cp "$TEMP_OUTPUT_FILE" "$OUTPUT" || err_quit "failed to copy '$TEMP_OUTPUT_FILE' to destination '$OUTPUT'"
echo "wrote $OUTPUT with JDK toolchains: ${UNIQUE_JDKS[*]}"
echo

# all done, offer build suggestion
print_build_suggestion "$BASE_JDK" "$BASE_JDK_HOME" "${UNIQUE_JDKS[*]}"

