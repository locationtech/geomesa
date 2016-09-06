#!/bin/bash
#
# Builds the HTML documentation tree and mirrors it to the geomesa.github.io repository.

# default paths for docs, can be overridden by command line switches
GEOMESA=/opt/devel/src/geomesa
GEOMESA_GITHUB_IO=/opt/devel/src/geomesa.github.io
DOCS=$GEOMESA_GITHUB_IO/documentation

function check_repo {
    # check that repo in $dir is a Git repo and has $url as a remote
    dir="$1"
    url="$2"
    echo "check_repo $dir $url"
    if [[ ! -d "$dir" ]] ; then
        echo "ERROR: $dir not a directory"
        exit 3
    fi
    if [[ ! -d "$dir/.git" ]] ; then
        echo "ERROR: $dir not a git repo"
        exit 3
    fi
    cwd=`pwd`
    cd $dir
    n=`git remote -v show 2>&1 | grep "$url" | wc -l`
    cd $cwd
    if [[ "$n" -gt 0 ]] ; then
        return 0
    else
        echo "ERROR: $dir does not have specified remote url"
        exit 3
    fi
}

function build_and_mirror {
    # build GeoMesa docs and mirror to geomesa.github.io repo
	branch="$1"
	release="$2"

	set -x
	set -e
	cd $GEOMESA && git checkout $branch
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

function usage {
    echo "$0: builds Sphinx documentation in \$GEOMESA and mirrors to \$GEOMESA_GITHUB_IO"
    echo
    echo "usage: $0 [options] <branch> <dest>"
    echo "    <branch>  branch or tag of \$GEOMESA to checkout and build"
    echo "    <dest>    directory in \$GEOMESA_GITHUB_IO/documentation to mirror to"
    echo "              (use 'root' to mirror to \$GEOMESA_GITHUB_IO/documentation)"
    echo
    echo "    options:"
    echo "        --gm <dir>       set \$GEOMESA [$GEOMESA]"
    echo "        --gmweb <dir>    set \$GEOMESA_GITHUB_IO [$GEOMESA_GITHUB_IO]"
    echo
}

### MAIN ###

# parse options
while :
do
   case "$1" in
       --gm)
           GEOMESA="$2"
           shift 2
           ;;
       --gmweb)
           GEOMESA_GITHUB_IO="$2"
           shift 2
           ;;
       -h | --help)
           usage
           exit 1
           ;;
       --) # manual end of options
           break
           ;;
       -*)
           echo "ERROR: unknown option $1" >&2
           exit 2
           ;;
       *) # end of options
           break
           ;;
   esac
done

# check command line arguments
if [[ $# -ne 2 ]] ; then
    usage
    exit 1
fi

# check repos and go
check_repo $GEOMESA git@github.com:locationtech/geomesa.git
check_repo $GEOMESA_GITHUB_IO git@github.com:geomesa/geomesa.github.io.git
build_and_mirror "$1" "$2"
