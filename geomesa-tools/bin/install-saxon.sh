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

url='http://search.maven.org/remotecontent?filepath=net/sf/saxon/Saxon-HE/9.7.0-14/Saxon-HE-9.7.0-14.jar'
read -r -p "Saxon is free to use and distribute, however, the provenance of the code could not be established by the Eclipse Foundation, and thus it is not distributed with GeoMesa... are you sure you want to install it from $url ? [Y/n] " confirm
confirm=${confirm,,} #lowercasing
if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
  echo "Trying to install Saxon from $url to ${lib_dir}"
  wget -O /tmp/Saxon-HE-9.7.0-14.jar $url \
    && mv /tmp/Saxon-HE-9.7.0-14.jar "${lib_dir}/" \
    && echo "Successfully installed Saxon to ${lib_dir}" \
    || { rm -f /tmp/Saxon-HE-9.7.0-14.jar; echo "Failed to download: ${url}"; };
else
  echo "Cancelled installation of Saxon"
fi
