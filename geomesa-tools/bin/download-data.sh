#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

type=$1

# configure HOME
export %%tools.dist.name%%_HOME="${%%tools.dist.name%%_HOME:-$(cd "$(dirname "$0")"/.. || exit; pwd)}"
data_dir="${%%tools.dist.name%%_HOME}/data"

NL=$'\n'
case "$type" in
  gdelt)
    read -r -p "Enter a date in the form YYYYMMDD: " DATE

    wget "http://data.gdeltproject.org/events/${DATE}.export.CSV.zip" -P "${data_dir}"/gdelt
    ;;

  geolife)
    wget "https://download.microsoft.com/download/F/4/8/F4894AA5-FDBC-481E-9285-D5F8C4C4F039/Geolife%20Trajectories%201.3.zip" -P "${data_dir}"/geolife
    ;;

  osm-gpx)
    echo "Available regions: africa, asia, austrailia-oceania, canada, central-america,europe, ex-ussr, south-america, usa"
    read -r -p "Enter a region to download tracks for: " CONTINENT

    wget "http://zverik.osm.rambler.ru/gps/files/extracts/$CONTINENT.tar.xz" -P "${data_dir}"/osm-gpx
    ;;

  tdrive)
    echo "Note: each zip file contains approximately one million points"
    read -r -p "Download how many zip files? (14 total) " NUM

    UA="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36"

    for i in $(seq 1 "$NUM"); do
      echo "Downloading zip $i of $NUM"
      wget "https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/0$i.zip" -P "${data_dir}"/tdrive -U "$UA" \
      || errorList+="https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/0${i}.zip ${NL}";
    done

    wget "https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/User_guide_T-drive.pdf" -P "${data_dir}"/tdrive \
      || errorList+="https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/User_guide_T-drive.pdf ${NL}";

    if [[ -n "${errorList}" ]]; then
      echo "Failed to download: ${NL} ${errorList[*]}";
    fi

    ;;

  geonames)
    read -r -p "Enter the country code to download data for: " CC

    wget "http://download.geonames.org/export/dump/$CC.zip" -P "${data_dir}"/geonames
    ;;

  *)
    if [[ -n "$type" ]]; then
      PREFIX="Unknown data type '$type'."
    else
      PREFIX="Please enter a data type."
    fi
    echo "${PREFIX} Available types: gdelt, geolife, osm-gpx, tdrive, geonames"
    ;;

esac
