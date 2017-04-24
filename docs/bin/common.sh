# Common functions used by documentation scripts

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
