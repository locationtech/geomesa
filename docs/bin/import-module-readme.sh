#!/bin/bash
#
# Import several README.md files from the GeoMesa source distribution,
# using ``pandoc`` to convert Markdown to RST.

# IMPORTANT: update these to match your system!
# (I had two copies of the geomesa repo because they were checked out on different branches)
GM_DOCS=/opt/devel/src/geomesa
GM_MASTER=/opt/devel/src/geomesa2
MODULES="
geomesa-compute
geomesa-convert
geomesa-jobs
geomesa-process
geomesa-raster
geomesa-stream
geomesa-utils
geomesa-web/geomesa-web-data
"

for module in $MODULES ; do
	readme=$GM_MASTER/$module/README.md
	module_out=`basename $module`
	rst=$GM_DOCS/docs/developer/modules/${module_out}.rst
	if [ -f $readme ] ; then
		pandoc -i $readme -o $rst
	else
		echo "$readme DOES NOT EXIST"
	fi
done
