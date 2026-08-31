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
        sh -c "$exiftool -overwrite_original_in_place -tagsFromFile $destfile $destthumbnail"
    fi

	# The iText hack flattens transparency so PDF reports render correctly. Only
	# formats that can carry an alpha channel need it. Running it on a JPEG is a
	# full decode/re-encode that changes nothing, strips the exif we then have to
	# restore, and degrades the image, so skip it.
	case `echo $ext | tr A-Z a-z` in
	jpg|jpeg)
		echo "skipping iText hack, $ext cannot have an alpha channel"
		;;
	*)
		echo "processing image file for iText hack also set background white"
		sh -c "convert -background white -alpha remove $destfile $destfile"

		if [ -n "$exiftool" ]; then
			echo "Preserving exif data in main file"
			sh -c "$exiftool -overwrite_original_in_place -tagsFromFile $destthumbnail $destfile"
		fi
		;;
	esac
fi

#If content type is "video" create a thumbnail 
if [ x"$type" = xvideo ]; then
	echo "--------------------------------------"
	echo "Creating thumbnails $destthumbnail from $destfile"
	rm $destthumbnail
	sh -c "ffmpeg -i $destfile -vf scale=-1:100  $destthumbnail"
fi

