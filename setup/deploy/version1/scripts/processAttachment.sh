#!/bin/sh

if [ $# -lt "4" ]; then
	echo "usage $0 filename directory content_type extension"
fi

echo "================================================="
echo "processing $0 $1 $2 $3 $4" 

filename=$1
destdir=$2
contenttype=$3
ext=$4
destfile="$destdir/$filename.$ext"
destthumbnail="$destdir/thumbs/$filename.$ext.jpg"


export PATH="$PATH:/usr/local/bin:/usr/bin"

# If content type is "image" create a thumbnail
type=`echo $contenttype | cut -c 1-5`
if [ x"$type" = ximage ]; then
	echo "--------------------------------------"
	echo "Creating thumbnails $destthumbnail from $destfile"
	rm $destthumbnail
	sh -c "convert -thumbnail 100 -background white -alpha remove $destfile $destthumbnail"
# Process the image file with a null processing action to address a bug in iText where some malformed jpegs can't be shown
	echo "processing image file for iText hack also set background white"
	sh -c "convert -background white -alpha remove $destfile $destfile"
fi

#If content type is "video" create a thumbnail 
if [ x"$type" = xvideo ]; then
	echo "--------------------------------------"
	echo "Creating thumbnails $destthumbnail from $destfile"
	rm $destthumbnail
	sh -c "ffmpeg -i $destfile -vf scale=-1:100  $destthumbnail"
fi

# If there is an s3 bucket available then send files to it
# Replaced with S3 API
#if [ -f /smap/settings/bucket ]; then
#
#        prefix="/smap"
#        region=`cat /smap/settings/region`
#
#	echo "Sending to aws bucket `cat /smap/settings/bucket`"
#        if [ -f  $destfile ]; then
#                relPath=${destfile#"$prefix"}
#                awsPath="s3://`cat /smap/settings/bucket`$relPath"
#                aws s3 --region $region cp $destfile $awsPath
#        fi
#        if [ -f  $destthumbnail ]; then
#                relPath=${destthumbnail#"$prefix"}
#                awsPath="s3://`cat /smap/settings/bucket`$relPath"
#                aws s3 --region $region cp $destthumbnail $awsPath
#        fi
#fi
