#! /usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
lib_dir="${%%gmtools.dist.name%%_HOME}/lib"

url='http://search.maven.org/remotecontent?filepath=jline/jline/2.12.1/jline-2.12.1.jar'
read -r -p "JLine is BSD licensed and free to use and distribute, however, the provenance of the code could not be established by the Eclipse Foundation, and thus it is not distributed with GeoMesa... are you sure you want to install it from $url ? [Y/n] " confirm
confirm=${confirm,,} #lowercasing
if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
  echo "Trying to install JLine from $url to ${lib_dir}"
  wget -O /tmp/jline-2.12.1.jar $url \
    && mv /tmp/jline-2.12.1.jar "${lib_dir}/" \
    && echo "Successfully installed JLine to ${lib_dir}" \
    || { rm -f /tmp/jline-2.12.1.jar; echo "Failed to download: ${url}"; };
else
  echo "Cancelled installation of JLine"
fi
