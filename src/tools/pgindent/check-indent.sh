#!/bin/bash

set -e
export PS4='+ [`basename ${BASH_SOURCE[0]}`:$LINENO ${FUNCNAME[0]} \D{%F %T} $$ ] '

CURDIR=$(cd "$(dirname "$0")"; pwd);
MYNAME="${0##*/}"
RET_SUCC=0
RET_FAIL=1

g_ACTION="format"

_check_os() {
    OS=`uname`
    OS=$(echo "$OS" | tr '[A-Z]' '[a-z]')

    if [ x$OS != "xlinux" ]; then
        echo "--- Only support linux, or you need to install pg_bsd_indent to src/tools/pgindent/pg_bsd_indent."
        exit $RET_FAIL
    fi
}

_usage() {
    cat << USAGE
Usage: bash ${MYNAME} [options] hostlist command.
Options:
    --check                Check code format for CI.
    -h, --help             Print this help infomation.

USAGE

    exit $RET_SUCC
}

#
# Parses command-line options.
#  usage: _parse_options "$@" || exit $?
#
_parse_options()
{
    declare -a argv

    while [[ $# -gt 0 ]]; do
        case $1 in
            --check)
                g_ACTION=check
            	shift
            	;;
            -h|--help)
            	_usage
            	exit
            	;;
            --)
            	shift
                argv=("${argv[@]}" "${@}")
            	break
            	;;
            -*)
            	echo "command line: unrecognized option $1" >&2
            	return 1
            	;;
            *)
                argv=("${argv[@]}" "${1}")
                shift
                ;;
        esac
    done
}

_main()
{
    echo "++++ start run pgindent... "

    _check_os

    ## get typedefs.list
    defsURL="https://buildfarm.postgresql.org/cgi-bin/typedefs.pl"
    wget -q -O $CURDIR/typedefs.list ${defsURL}
    if [ $? -ne 0 ]; then
        echo "--- wget ${defsURL} failed."
        exit ${RET_FAIL}
    fi

    export PATH=$CURDIR:$PATH
    if command -v pg_bsd_indent >/dev/null; then
        :
    elif command -v pg_bsd_indent >/dev/null; then
        echo "--- please install pg_bsd_indent first."
        exit ${RET_FAIL}
    fi

    if [ ! -x $CURDIR/pgindent ]; then
        echo "--- please check $CURDIR/pgindent existed and have exec right."
        exit ${RET_FAIL}
    fi

    $CURDIR/pgindent
    if [ $? -ne 0 ]; then
        echo "--- run pgindent failed."
        exit ${RET_FAIL}
    fi

    if [ x${g_ACTION} == "xcheck" ]; then
        ret=`git status | grep modified | wc -l`
        if [ "x$ret" != "x0" ]; then
            echo "--- please check code format and run pgindent."
            git status
            exit ${RET_FAIL}
        fi
    fi

    echo "++++ finish run pgindent... "
}

################################## main route #################################
_parse_options "${@}" || _usage

_main
