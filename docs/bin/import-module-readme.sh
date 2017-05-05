#!/bin/bash
#
# Import several README.md files from the GeoMesa source distribution,
# using ``pandoc`` to convert Markdown to RST.
#
# ----------------------------------------------------------------------
# WARNING: This is not completely automated. You will almost certainly
# have to hand-edit the files after this import!
# ----------------------------------------------------------------------

# default paths
# may be overridden by setting the $GEOMESA variable
# (will try to guess relative to the script path)
GEOMESA=${GEOMESA:-$(readlink -f "$(dirname ${BASH_SOURCE[0]})/../..")}

MODULES="
geomesa-metrics:metrics.rst
geomesa-process:process.rst
geomesa-accumulo/geomesa-accumulo-raster:accumulo/raster.rst
geomesa-utils:utils.rst
"

for module in $MODULES ; do
    IFS=':' read -ra part <<< "$module"
	readme=$GEOMESA/${part[0]}/README.md
	rst=$GEOMESA/docs/user/${part[1]}
	if [ -f $readme ] ; then
		pandoc -i $readme -o $rst
	else
		echo "$readme DOES NOT EXIST"
	fi
done
