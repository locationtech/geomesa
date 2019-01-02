#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

type=$1

if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
data_dir="${%%gmtools.dist.name%%_HOME}/data"

NL=$'\n'
case "$type" in
  gdelt)
    read -p "Enter a date in the form YYYYMMDD: " DATE

    wget "http://data.gdeltproject.org/events/${DATE}.export.CSV.zip" -P ${data_dir}/gdelt
    ;;

  geolife)
    wget "http://ftp.research.microsoft.com/downloads/b16d359d-d164-469e-9fd4-daa38f2b2e13/Geolife Trajectories 1.3.zip" -P ${data_dir}/geolife
    ;;

  osm-gpx)
    echo "Available regions: africa, asia, austrailia-oceania, canada, central-america,europe, ex-ussr, south-america, usa"
    read -p "Enter a region to download tracks for: " CONTINENT

    wget "http://zverik.osm.rambler.ru/gps/files/extracts/$CONTINENT.tar.xz" -P ${data_dir}/osm-gpx
    ;;

  tdrive)
    echo "Note: each zip file contains approximately one million points"
    read -p "Download how many zip files? (14 total) " NUM

    UA="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36"

    for i in `seq 1 $NUM`; do
      echo "Downloading zip $i of $NUM"
      wget "https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/0$i.zip" -P ${data_dir}/tdrive -U "$UA" \
      || errorList="${errorList[@]} https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/0${i}.zip ${NL}";
    done

    wget "https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/User_guide_T-drive.pdf" -P ${data_dir}/tdrive \
      || errorList="${errorList[@]} https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/User_guide_T-drive.pdf ${NL}";

    if [[ -n "${errorList}" ]]; then
      echo "Failed to download: ${NL} ${errorList[@]}";
    fi

    ;;

  geonames)
    read -p "Enter the country code to download data for: " CC

    wget "http://download.geonames.org/export/dump/$CC.zip" -P ${data_dir}/geonames
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
