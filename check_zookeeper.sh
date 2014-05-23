#!/bin/bash
# vim: ts=4:sw=4:et:sts=4:ai:tw=80
# Argument = -t test -r server -p password -v

usage()
{
cat << EOF
usage: $0 options

This script run the test1 or test2 over a machine.

OPTIONS:
   -c      Cache file to store kerberos credentials
   -h      Show this message
   -H      Zookeeper server list with port
   -K      Keytab to use for kerberos credentials
   -P      Principal to use for kerberos credentials
   -s      Kerberized zookeeper
   -t      Test type, can be '' or ''
EOF
}

OK=0
WARNING=1
CRITICAL=2
UNKNOWN=3
ZK_CMD="/usr/bin/zookeeper-client"

SECURE=false
CACHE_FILE="/tmp/krb_`basename $0`"
PRINCIPAL=
KEYTAB=
SERVER=
TEST=

HBASE_ZNODES=('/hbase/master 1' '/hbase/backup-masters 1' '/hbase/meta-region-server 1' '/hbase/unassigned 0')
HDFS=('/hadoop-ha/hdfscluster/ActiveStandbyElectorLock 1')


parser() {
    while getopts "c:hH:K:P:st:" OPTION
    do
        case "$OPTION" in
            c) 
                CACHE_FILE=$OPTARG
                ;;
            h)
                usage
                exit 1
                ;;
            H)
                SERVER=$OPTARG
                ;;
            K)
                KEYTAB=$OPTARG
                ;;
            P)
                PRINCIPAL=$OPTARG
                ;;
            s)
                SECURE=true
                ;;
            t)
                TEST=$OPTARG
                ;;
            ?)
                usage
                exit
                ;;
        esac
    done

    if [[ -z "$TEST" ]] || [[ -z "$SERVER" ]]
    then
        usage
        exit 1
    fi

    if  "$SECURE" && ( [[ -z "$PRINCIPAL" ]] || [[ -z "$KEYTAB" ]] )
    then
        usage
        exit 1
    fi
}

auth(){
    code=$OK
    msg="No authentication defined"
    if "$SECURE";then 
        kinit -kt "$KEYTAB" "$PRINCIPAL" &>/dev/null
        rc="$?"
        if [[ $rc -ne 0 ]]; then
            code=$UNKNOWN
            msg="Authentication fails"
        else
            code=$OK
            msg="Principal authenticated"
        fi

    fi
    echo "code=$code;msg='$msg'"
}

resume(){
    case $1 in 
        $OK)
            msg="OK - $2"
            ;;
        $WARNING)
            msg="WARNING - $2"
            ;;
        $CRITICAL)
            msg="CRITICAL - $2"
            ;;
        *)
            msg="UNKNOWN - $2" 
            ;;
    esac
    echo $msg
    return $1
}

check_znode_exists(){
    path=$1
    created=$2
    code=$OK
    msg="Node exists"
    retval=$(($ZK_CMD -server $SERVER stat $path)2>&1)
    while read -r line; do
        if [[ "$line" = "Node does not exist:"* ]] ; then
            code=$CRITICAL
            msg="Z$line"
            break
        fi
    done <<< "$retval"
    if [[ $created -eq 0 ]]; then
        code=$(( $CRITICAL - $code ))
    fi
    echo "code=$code;msg='$msg'"
}

main(){
    parser $@
    [[ $SECURE ]] && CACHE_FILE_ORIG=$KRB5CCNAME && export KRB5CCNAME=$CACHE_FILE
    auth_res=$(auth)
    eval $auth_res
    if [[ $code -ne 0 ]]; then
        resume "$code" "$msg"
    else
        case "$TEST" in
            "hbase")
                for i in `seq 0 $(( ${#HBASE_ZNODES[@]} - 1 ))`; do
                    val=$(check_znode_exists ${HBASE_ZNODES[$i]})
                    eval $val
                    [[ $code -ne 0 ]] && break
                done
                ;;
            "hdfs")
                echo $TEST
                ;;
        esac
    fi
    [[ $SECURE ]] && kdestroy -c $CACHE_FILE && export KRB5CCNAME=$CACHE_FILE_ORIG
    resume "$code" "$msg"
}

main $@
