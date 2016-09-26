#!/usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

NL=$'\n'
TAB=$'\t'
usage="usage: ./validate-jars.sh [install_dir] [gs_plugin_dir] [<options>] | [-h|--help]"

# Parse options
if [[ "$1" == "--help" || "$1" == "-h" || $# -eq 0 ]]; then
  echo ""
  echo "This script allows you to easily manage GeoMesa's Geoserver plugins."
  echo ""
  echo "${usage}"
  echo ""
  echo "Required:"
  echo "  -l,--lib-dir,\$1      Path to Geoserver WEB-INF/lib directory"
  echo "  -p,--plugin-src,\$2   Path to GeoMesa's Geoserver plugins directory (geomesa/geomesa-gs-plugin)"
  echo ""
  echo "Options:"
  echo "  -c,--compare         Print comparison of jars in the lib-dir"
  echo "                       and jars in the plugin-dir directory"
  echo "  -g,--upgrade         Upgrade requires a path to the old geomesa/geomesa-gs-plugin dir."
  echo "                       Jars found in the old plugin dir will be removed and jars from the"
  echo "                       new plugin dir will be installed. See example below. This is equivalent"
  echo "                       to using --uninstall and --install sequentially."
  echo "  -i,--install         Install jars from the plugin-dir to the Geoserver lib-dir"
  echo "  -u,--uninstall       Remove jars from the Geoserver lib-dir that are in the plugin-dir."
  echo "                       Note this will only uninstall jars with an exact version match."
  echo "  -r,--repair          Attempts to uninstall Geomesa's Geoserver plugins and then re-install them."
  echo "                       This differs from --upgrade by using fuzzy version matching to remove any jar"
  echo "                       related to this version of Geomesa. This may cause issues with other software"
  echo "                       running on this Geoserver that requires certain versions of jars."
  echo "  -v,--validate        Reports if the installed jars match the jars in the given gs-plugin dir."
  echo ""
  exit 0
else
  if [[ -d "$1" && -d "$2" ]]; then
    install_dir=$1
    gs_plugin_dir=$2
    shift 2
  fi
  while [[ $# -gt 0 ]]; do
    key="$1"
    cc=0 # Command Count
    case $key in
      -l|--lib-dir)
        install_dir="$2"
        shift
      ;;
      -p|--plugin-src)
        gs_plugin_dir="$2"
        shift
      ;;
      -c|--compare)
        cc=$(expr $cc + 1)
        command="compare"
      ;;
      -g|--upgrade)
        cc=$(expr $cc + 1)
        command="upgrade"
        old_gs_plugin_dir="$2"
        shift
      ;;
      -i|--install)
        cc=$(expr $cc + 1)
        command="install"
      ;;
      -u|--uninstall)
        cc=$(expr $cc + 1)
        command="uninstall"
      ;;
      -r|--repair)
        cc=$(expr $cc + 1)
        command="repair"
      ;;
      -v|--validate)
        cc=$(expr $cc + 1)
        command="validate"
      ;;
      *)
        echo "Unknown parameter $1"
        echo "${usage}"
        exit 1
      ;;
    esac
    shift
  done
  # Parameter error checking
  if [[ ! "${cc}" == "1" ]]; then
    echo "Invalid argument set. Must use one and only one option. "
    echo "${usage}"
    exit 1
  fi
  if [[ ! -d "${install_dir}" ]]; then
    echo "Invalid Geoserver plugin install dir"
    echo "${usage}"
    exit 1
  fi
  if [[ ! -d "${gs_plugin_dir}" ]]; then
    echo "Invalid Geomesa-Geoserver plugin source dir"
    echo "${usage}"
    exit 1
  fi
  if [[ "${command}" == "upgrade" && ! -d "${old_gs_plugin_dir}" ]]; then
    echo "Invalid old Geomesa-Geoserver plugin source dir"
    echo "${usage}"
    exit 1
  fi
fi

# Global declarations
declare -a matching
declare -a mismatched
declare -a not_matching

# Get list of installed jars
echo "Collecting Installed Jars"
installed_jars=($(ls -l $install_dir | awk '{print $9}' | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/ /g'))

# Get lists of jars in geomesa-gs-plugin tars
echo "Collecting geomesa-gs-plugin Jars"
pushd "${gs_plugin_dir}" > /dev/null # Temp use this dir so pwd gives full path to tar
  gs_plugin_tars=$(find `pwd` -type f -name *.tar.gz)
popd > /dev/null
for tar_path in $gs_plugin_tars; do
  gs_plugin_jars=("${gs_plugin_jars[@]}" $(tar tf "${tar_path}"))
done

# Get unique and sort
gs_plugin_jars=($(echo "${gs_plugin_jars[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))
installed_jars=($(echo "${installed_jars[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))

# Functions
function containsElement() {
  local var
  for var in "${@:2}"; do [[ "${var}" == "$1" ]] && return 0; done
  return 1
}

function removeJars() {
  install_dir="${1}"
  jars=("${@}")
  echo "Jars to be removed:"
  for jar in "${jars}"; do
    echo "${jar}"
  done
  read -p "Are you sure you want to Continue? [Y/n]" confirm
  confirm=${confirm,,} #lowercasing
  if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
    echo "Removing jars from ${NL}${install_dir}"
    for jar in "${jars}"; do
      rm -f "${install_dir}/${jar}"
    done
  else
    echo "Aborting"
    exit 1
  fi
}

function addJars() {
  install_dir="${1}"
  gs_plugin_dir="${2}"
  tar_options=()

  # Collect tars
  pushd "${gs_plugin_dir}" > /dev/null# Temp use this dir so pwd gives full path to tar
    gs_plugin_tars=($(find `pwd` -type f -name *.tar.gz | sed -e 's/\n/ /g'))
  popd > /dev/null
  for tar in "${gs_plugin_tars[@]}"; do
    tar_option="$(echo "$(basename $tar)" | sed -e 's/.tar.gz//' -e 's/-install//' -e 's/geomesa-//')"
    tar_options=("${tar_options[@]}" "${tar_option}")
  done

  # Ask user what they want installed
  echo "Please choose which modules to install"
  echo "Multiple may be specified: 1 4 10"
  echo "Type 'a' to specify all"
  echo "--------------------------------------"
  for i in "${!tar_options[@]}"; do
    echo -e "${i} | ${tar_options[$i]}"
  done
  read -p "Module(s) to install: " -a options

  # Run install
  if [[ "${options[0]}" =~ ^(a|A) ]]; then
    for tar in "${gs_plugin_tars[@]}"; do
      echo "Installing $(basename $tar)"
      tar -xf "${tar}" -C "${install_dir}"
    done
  else
    for option in "${options[@]}"; do
      echo "Installing $(basename ${gs_plugin_tars[$option]})"
      tar -xf "${gs_plugin_tars[$option]}" -C "${install_dir}"
    done
  fi
  echo "Done"
}

function stripVersion() {
  jar_list=("${!1}")
  jar_list_nv=()
  for jar in "${jar_list[@]}"; do
    jar_list_nv="${jar_list_nv[@]} $(echo "${jar}" | sed 's/\(-[0-9]\(\.[0-9]\)*-\?[-.0-9a-zA-Z]*\).jar$//')"
  done
  echo "${jar_list_nv[@]}"
}

function buildComparison() {
  jar_list_1=("${!1}")
  jar_list_2=("${!2}")

  matching=()
  mismatching=()
  notmatching=()
  jar_list_1_nv=($(stripVersion jar_list_1[@]))
  jar_list_2_nv=($(stripVersion jar_list_2[@]))

  echo "Building Jar Comparison"
  for i in "${!jar_list_1[@]}"; do
    found="false"
    for j in "${!jar_list_2[@]}"; do
      if [[ "${jar_list_1[$i]}" == "${jar_list_2[$j]}" ]]; then
        matching=("${matching[@]}" "${jar_list_1[$i]}")
        found="true"
        break
      elif [[ "${jar_list_1_nv[$i]}" == "${jar_list_2_nv[$j]}" ]]; then
        mismatching=("${mismatching[@]}" "${jar_list_1[$i]}" "${jar_list_2[$j]}")
        found="true"
        break
      fi
    done
    if [[ "${found}" == "false" ]]; then
      notmatching=("${notmatching[@]}" "${jar_list_1[$i]}")
    fi
  done
}

function padString() {
  # padString [length] [string]
  length="$1"
  string="$2"
  padded="${string}"
  if [[ ${#string} -gt ${length} ]]; then
    echo "${string}"
    return
  fi
  j=0
  while [[ $j -lt $(expr $length - ${#string}) ]]; do
    padded="${padded} "
    j=$(expr $j + 1)
  done
  echo "${padded}"
}

function printArrs() {
  direction=$1
  shift
  matching=("${!1}")
  mismatching=("${!2}")
  notmatching=("${!3}")

  # Print matching results
  echo "=========== Matching =========="
  echo "These jars were found in both locations${NL}"
  for match in "${matching[@]}"; do
    echo "${match}"
  done

  # Print mismatching results
  echo "${NL}"
  echo "===== Mismatched versions =====${NL}"
  # Get length of longest jar name
  length=0
  for name in "${mismatching[@]}"; do
    if [[ ${#name} -gt length ]]; then length=${#name}; fi
  done
  # Print direction info
  if [[ "${direction}" == "forward" ]]; then
    padded=$(padString ${length} "Installed")
    echo -e "${padded}\t| Available"
  else
    padded=$(padString ${length} "Available")
    echo -e "${padded}\t| Installed"
  fi
  i=0
  while [[ $i -lt $length ]]; do
    sep="${sep}-"
    i=$(expr $i + 1)
  done
  echo "${sep}----|${sep}" # extra '-' for \t
  i=0
  while [[ $i -lt ${#mismatching[@]} ]]; do
    padded=$(padString ${length} ${mismatching[$i]})
    echo -e "${padded}\t| ${mismatching[$i+1]}"
    i=$(expr $i + 2)
  done

  # Print notmatching results
  echo "${NL}"
  echo "======== Not matching ========="
  # Print direction info
  if [[ "${direction}" == "forward" ]]; then
    echo "Other installed jars${NL}"
  else
    echo "These jars available for install${NL}"
  fi
  for notmatch in "${notmatching[@]}"; do
    echo "${notmatch}"
  done
}

# Control Structure
if [[ "${command}" == "compare" ]]; then
  buildComparison installed_jars[@] gs_plugin_jars[@]
  printArrs "forward" matching[@] mismatching[@] notmatching[@]
elif [[ "${command}" == "upgrade" ]]; then
  # Get lists of jars in old geomesa-gs-plugin tars
  echo "Collecting old geomesa-gs-plugin Jars"
  pushd "${old_gs_plugin_dir}" > /dev/null
    old_gs_plugin_tars=$(find `pwd` -type f -name *.tar.gz)
  popd > /dev/null
  for tar_path in $gs_plugin_tars; do
    old_gs_plugin_jars=("${old_gs_plugin_jars[@]}" $(tar tf "${tar_path}"))
  done
  # Get unique and sort
  old_gs_plugin_jars=($(echo "${old_gs_plugin_jars[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))
  # Remove old jars
  removeJars "${install_dir}" "${old_gs_plugin_jars[@]}"
  # Add new jars
  addJars "${install_dir}" "${gs_plugin_dir[@]}"
elif [[ "${command}" == "install" ]]; then
  addJars "${install_dir}" "${gs_plugin_dir}"
elif [[ "${command}" == "uninstall" ]]; then
  removeJars "${install_dir}" "${gs_plugin_jars[@]}"
elif [[ "${command}" == "repair" ]]; then
  echo "Attempting to repair Geomesa's Geosever plugins"
  read -p "Are you sure you want to continue? This cannot be undone. [y/N]" confirm
  confirm=${confirm,,}
  if [[ $confirm =~ ^(yes|y) ]]; then
    echo "${NL}"
    echo "Removing:"
    # Uninstall jars
    gs_plugin_jars_nv=($(stripVersion gs_plugin_jars[@]))
    installed_jars_nv=($(stripVersion installed_jars[@]))
    for i in "${!gs_plugin_jars[@]}"; do
      for j in "${!installed_jars[@]}"; do
        # Look for a fuzzy match for all installed jars and remove them
        if [[ "${gs_plugin_jars_nv[$i]}" == "${installed_jars_nv[$j]}" ]]; then
          echo "${installed_jars[$j]}"
          rm -f "${install_dir}/${installed_jars[$j]}"
          add_list=("${add_list[@]}" "${gs_plugin_jars[$i]}") # add_list may have duplicate jars
        fi
      done
    done
    echo "${NL}"
    echo "Installing:"
    # Install jars
    for tar_path in $gs_plugin_tars; do
      # Look in each tar for a jar we removed and if one is found install the whole tar
      jar_list=($(tar tf "${tar_path}"))
      install_tar="false"
      for j in "${!add_list[@]}"; do
        if containsElement "${add_list[$j]}" "${jar_list[@]}"; then
          install_tar="true"
          break
        fi
      done
      if [[ "${install_tar}" == "true" ]]; then
        echo "$(basename ${tar_path})"
        tar -xf "${tar_path}" -C "${install_dir}"
      fi
    done
  fi
elif [[ "${command}" == "validate" ]]; then
  # Use this order so notmatching contains missing jars
  buildComparison gs_plugin_jars[@] installed_jars[@]

  out=0
  if [[ "${#mismatching[@]}" -ne "0" ]]; then
    echo "** There are new versions of jars to be installed. **"
    out=1
  fi
  if [[ "${#notmatching[@]}" -ne "0" ]]; then
    echo "** There are new jars that need to be installed. **"
    out=1
  fi

  if [[ "${out}" == "0" ]]; then
    echo "GeoMesa's Geoserver plugins appear to be properly installed."
  else
    read -p "Print results of comparison? [y/N]" confirm
    confirm=${confirm,,}
    if [[ $confirm =~ ^(yes|y) ]]; then
      printArrs "backward" matching[@] mismatching[@] notmatching[@]
    fi
  fi
fi