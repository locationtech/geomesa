#! /usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

type=$1

setGeoHome ()
{
    SOURCE="${BASH_SOURCE[0]}"
    # resolve $SOURCE until the file is no longer a symlink
    while [ -h "${SOURCE}" ]; do
        bin="$( cd -P "$( dirname "${SOURCE}" )" && pwd )"
        SOURCE="$(readlink "${SOURCE}")"
        # if $SOURCE was a relative symlink, we need to resolve it relative to the path where
        # the symlink file was located
        [[ "${SOURCE}" != /* ]] && SOURCE="${bin}/${SOURCE}"
    done
    bin="$( cd -P "$( dirname "${SOURCE}" )" && cd ../ && pwd )"
    export GEOMESA_HOME="$bin"
    export PATH=${GEOMESA_HOME}/bin:$PATH
    echo >&2 "Warning: GEOMESA_HOME is not set, using $GEOMESA_HOME"
}

if [ -z "$GEOMESA_HOME" ]; then
  setGeoHome
fi

case "$type" in
  gdelt)
    read -p "Enter a date in the form YYYYMMDD: " DATE

    wget "http://data.gdeltproject.org/events/${DATE}.export.CSV.zip" -P $GEOMESA_HOME/data/gdelt
    ;;

  geolife)

    wget "http://ftp.research.microsoft.com/downloads/b16d359d-d164-469e-9fd4-daa38f2b2e13/Geolife Trajectories 1.3.zip" -P $GEOMESA_HOME/data/geolife
    ;;

  osm-gpx)

    echo "Available regions: africa, asia, austrailia-oceania, canada, central-america,europe, ex-ussr, south-america, usa"
    read -p "Enter a region to download tracks for: " CONTINENT

    wget "http://zverik.osm.rambler.ru/gps/files/extracts/$CONTINENT.tar.xz" -P $GEOMESA_HOME/data/osm-gpx
    ;;

  tdrive)

    echo "Note: each zip file contains approximately one million points"
    read -p "Download how many zip files? (9 total) " NUM

    for i in `seq 1 $NUM`; do
      echo "Downloading zip $i of $NUM"
      ZIP_ID=$((i+5))
      wget "https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/0$ZIP_ID.zip" -P $GEOMESA_HOME/data/tdrive
      sleep 5
    done

    wget "https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/User_guide_T-drive.pdf" -P $GEOMESA_HOME/data/tdrive
    ;;

  geonames)

    read -p "Enter the country code to download data for: " CC

    wget "http://download.geonames.org/export/dump/$CC.zip" -P $GEOMESA_HOME/data/geonames
    ;;

  *)

    if [ -n "$type" ]; then
      PREFIX="Unknown data type '$type'."
    else
      PREFIX="Please enter a data type."
    fi
    echo "${PREFIX} Available types: gdelt, geolife, osm-gpx, tdrive, geonames"
    ;;

esac
