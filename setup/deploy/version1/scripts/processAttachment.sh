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


export PATH="$PATH:/usr/local/bin:/usr/bin:/bin:/snap/bin:/opt/homebrew/bin"

# Resolve exiftool location (varies by OS / install method: apt, snap, brew)
exiftool=`command -v exiftool`
if [ -z "$exiftool" ]; then
	echo "WARNING: exiftool not found on PATH ($PATH) - exif data will not be preserved. Install with: apt-get install libimage-exiftool-perl"
fi

# If content type is "image" create a thumbnail
type=`echo $contenttype | cut -c 1-5`
if [ x"$type" = ximage ]; then
	echo "--------------------------------------"
	echo "Creating thumbnails $destthumbnail from $destfile"
	rm $destthumbnail
	sh -c "convert -thumbnail 100 -background white -alpha remove $destfile $destthumbnail"

    if [ -n "$exiftool" ]; then
        echo "Preserve exif data in thumbnail"
        sh -c "$exiftool -v -overwrite_original_in_place -tagsFromFile $destfile $destthumbnail"
    fi

   	echo "processing image file for iText hack also set background white"
	sh -c "convert -background white -alpha remove $destfile $destfile"

	if [ -n "$exiftool" ]; then
		echo "Preserving exif data in main file"
		sh -c "$exiftool -v -overwrite_original_in_place -tagsFromFile $destthumbnail $destfile"
	fi
fi

#If content type is "video" create a thumbnail 
if [ x"$type" = xvideo ]; then
	echo "--------------------------------------"
	echo "Creating thumbnails $destthumbnail from $destfile"
	rm $destthumbnail
	sh -c "ffmpeg -i $destfile -vf scale=-1:100  $destthumbnail"
fi

