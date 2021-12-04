#!/bin/sh

if [ $# -lt "1" ]; then
	echo "usage $0 filepath"
fi

echo "================================================="
echo "processing $0 $1" 

filepath=$1

# If there is an s3 bucket available then send the file to it`
# TODO use base path
if [ -f /smap/settings/bucket ]; then

        prefix="/smap"
        region=`cat /smap/settings/region`

        if [ -f  $filepath ]; then
                relPath=${filepath#"$prefix"}
                awsPath="s3://`cat /smap/settings/bucket`$relPath"
                /usr/bin/aws s3 --region $region cp $filepath $awsPath
        fi
fi
