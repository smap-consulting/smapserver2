#!/bin/sh

if [ $# -lt "2" ]; then
        echo "usage $0 survey_ident attachments | uploadedSurveys"
fi

exitValue=0

echo "================================================="
echo "processing $0 $1 $2" 

#TODO use base path
if [ -f /smap/settings/bucket ]; then
	ident=$1
	type=$2
	restoreDir="/smap/$type/$ident"
	awsPath="s3://`cat /smap/settings/bucket`/$type/$ident"
	region=`cat /smap/settings/region`
	
	/usr/bin/aws s3 sync --region $region $awsPath $restoreDir
	exitValue=$?
fi
	
exit $exitValue
	
