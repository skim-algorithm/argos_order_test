#!/bin/sh
jobname=$1
container=$2
CURL='/usr/bin/curl'
CURLARGS='-f -s -S -k'
joburl="http://10.178.0.22:8080/job/${jobname}/lastStableBuild/api/json"
response=$(curl -s $joburl | jq -r .result)
url=https://asia-northeast3-r-arques-alphasystem.cloudfunctions.net/slack_noti
channel=%23build
space=%20
newline=%0A
now=$(date +%F_%H:%M:%S)
if [ $container -eq '' ]
then
    message="${now}${newline}All${space}build${space}result=${response}${newline}dev-arques-order-instance"
else
    message="${now}${newline}${container}${space}build${space}result=${response}${newline}dev-arques-order-instance"
fi
request="${url}?channel=${channel}&message=${message}"
raw="$($CURL $CURLARGS $request)"
exit 0
