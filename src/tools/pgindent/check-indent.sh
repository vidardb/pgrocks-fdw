#!/bin/sh

set -e -x
export PS4='+ [`basename ${BASH_SOURCE[0]}`:$LINENO ${FUNCNAME[0]} \D{%F %T} $$ ] '

CURDIR=$(cd "$(dirname "$0")"; pwd);
MYNAME="${0##*/}"
RET_SUCC=0
RET_FAIL=1

## get typedefs.list
wget -O $CURDIR/typedefs.list https://buildfarm.postgresql.org/cgi-bin/typedefs.pl
if [ $? -ne 0 ]; then
    echo "--- wget https://buildfarm.postgresql.org/cgi-bin/typedefs.pl failed."
    exit ${RET_FAIL}
fi

echo "++++ start run pgindent... "
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

ret=`git status | grep modified | wc -l`
if [ "x$ret" != "x0" ]; then
    echo "--- please check code format and run pgindent."
    git status
    exit ${RET_FAIL}
fi
    
