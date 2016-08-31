#!/bin/bash

#default parameters
s3cfgfile=~/.s3cfg-redborder
s3bucket="redborder"
topic="rb_flow_post"
namespace="default"
date=""
filter=""
datasource="rb_flow"

function help() {
    echo "USAGE:"
    echo "get_events_size.sh <OPTIONS>"
    echo " s) s3cfg_file (default: ~/.s3cfg-redborder)"
    echo " b) bucket (default: redborder)"
    echo " t) topic (default: rb_flow_post)"
    echo " n) namespace (default: default)"
    echo " a) datasource (default: rb_flow)"
    echo " d) date (REQUIRED)"
    echo " f) filter (OPTIONAL). Example format: 2016-08-25"
    echo " h) show help"
}

while getopts "s:b:t:n:d:f:a:h" opt ; do
    case $opt in
       	s) s3cfg_file=$OPTARG;;
        b) s3bucket=$OPTARG;;
        t) topic=$OPTARG;;
        n) namespace=$OPTARG;;
        a) datasource=$OPTARG;;
        d) date=$OPTARG;;
        f) filter=$OPTARG;;
        h) help; exit 0;;
    esac
done
if [ "x$date" = "x" ] ; then
    echo "Error: date is required"
    help
else
    if [ -d "raw_data" ] ; then
        echo "raw data already obtained"
        pushd raw_data &> /dev/null
    else
        mkdir -p raw_data
        pushd raw_data &> /dev/null

        echo -n "Getting raw data from s3 "
        s3cmd get -c $s3cfgfile --recursive s3://$s3bucket/rbraw/$topic/$namespace/dt=$date/ > /dev/null
        if [ $? -eq 0 ] ; then echo "ok" ; else echo "failed" ; popd &> /dev/null ; exit 1 ; fi

        echo -n "Decompressing raw data... "
        gzip -d -r *
        if [ $? -eq 0 ] ; then echo "ok" ; else echo "failed" ; popd &> /dev/null ; exit 1 ; fi

    fi

    echo
    #Obtaining segment size from historicals
    rm -f ../query.json
    cat > ../query.json <<- _RBEOF2_
{
  "queryType":"segmentMetadata",
  "dataSource":"$datasource",
  "intervals":["$date/$(date -d "2016-08-25+1 days" +%Y-%m-%d)"]
}
_RBEOF2_
    historical_size_query=$(curl -sX POST http://$(rb_get_druid_brokers.rb -r)/druid/v2/?pretty=true -H 'content-type: application/json'  -d @../query.json)

    count=0
    printf "HOUR\t\tEVENTS\t\tRAW_SIZE(MB)\tS3_SIZE(MB)\tHIST_SIZE(MB)\n"
    for hour in $(ls -1) ; do

        printf "$hour\t\t"

        rm -f $hour/filtered.json
        if [ "x$filter" = "x" ] ; then
            cat $hour/* > $hour/filtered.json
        else
            grep $filter $hour/* > $hour/filtered.json
        fi

        num_events=$(wc -l $hour/filtered.json | awk '{print $1}')
        printf "$num_events\t\t"

        size=$(du -sh -m $hour/filtered.json | awk '{print $1}')
        printf "$size\t\t"

        simple_hour=$(echo $hour | cut -c4-5)
        s3_size=$(s3cmd ls -c $s3cfgfile s3://$s3bucket/rbdata/$datasource/${date}T${simple_hour}:00:00.000Z_${date}T$(printf "%2d" $(echo "$simple_hour+1" | bc) | tr ' ' '0'):00:00.000Z/ --recursive | grep index.zip | awk '{print $3}' | awk '{s+=$1} END { printf "%.0f", s}')
        s3_size_mb=$(echo $s3_size | awk '{s=$1/1024/1024} END { printf "%.0f",s}')
        printf "$s3_size_mb\t\t"

        hist_size=$(echo $historical_size_query | jq -r .[$count].size | awk '{s=$1/1024/1024; printf "%.0f\n",s}')
        printf "$hist_size\n"

        let count=count+1
    done

    popd &> /dev/null
fi
