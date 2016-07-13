#!/bin/bash
#
# Builds the HTML documentation tree and mirrors it to the geomesa.github.io repository.

# IMPORTANT: Update these paths for your system!
GEOMESA=/opt/devel/src/geomesa
GEOMESA_GITHUB_IO=/opt/devel/src/geomesa.github.io
DOCS=$GEOMESA_GITHUB_IO/documentation

function build_and_mirror {
	branch=$1
	release=$2

	set -x
	cd $GEOMESA && git checkout $branch && git fpp
	mvn clean install -Pdocs -f $GEOMESA/docs
	set +x
	if [ "$release" == "root" ] ; then
		set -x
		rsync -av $GEOMESA/docs/target/html/ $GEOMESA_GITHUB_IO/documentation/
		set +x
	else
		set -x
		mkdir -p $GEOMESA_GITHUB_IO/documentation/$release
		rsync -av --delete $GEOMESA/docs/target/html/ $GEOMESA_GITHUB_IO/documentation/$release/
		set +x
	fi
}

#build_and_mirror wip_docs_120_backports 1.2.0
#build_and_mirror geomesa-1.2.1 
#build_and_mirror wip_docs_122_backports 1.2.2
#build_and_mirror wip_docs_123_backports 1.2.3
build_and_mirror wip_docs_124_backports 1.2.4
build_and_mirror wip_docs_123_backports root
