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
	echo "WARNING: exiftool not found on PATH ($PATH) - exif data will not be preserved. Install the exiftool package"
fi

# ImageMagick 7 is called magick, convert is the version 6 name and is deprecated in 7.
# The input file comes first in each command, which both versions accept
im=`command -v magick || echo convert`

# If content type is "image" create a thumbnail
type=`echo $contenttype | cut -c 1-5`
if [ x"$type" = ximage ]; then
	echo "--------------------------------------"
	echo "Creating thumbnails $destthumbnail from $destfile"
	rm -f $destthumbnail

	# The iText hack flattens transparency so PDF reports render correctly, and
	# the exiftool calls exist only to restore the exif that convert strips.
	# Neither is needed for jpeg: it cannot carry an alpha channel, so the hack
	# is a full decode/re-encode that changes nothing and degrades the image,
	# and -auto-orient bakes the exif rotation into the thumbnail pixels so the
	# orientation tag no longer has to be copied back in. That drops two convert
	# passes and two exiftool processes per photo, on the submission thread.
	case `echo $ext | tr A-Z a-z` in
	jpg|jpeg)
		echo "$ext needs no iText hack or exif copy, orienting thumbnail in place"
		sh -c "$im $destfile -auto-orient -thumbnail 100 -background white -alpha remove $destthumbnail"
		;;
	*)
		sh -c "$im $destfile -thumbnail 100 -background white -alpha remove $destthumbnail"

		if [ -n "$exiftool" ]; then
			echo "Preserve exif data in thumbnail"
			sh -c "$exiftool -overwrite_original_in_place -tagsFromFile $destfile $destthumbnail"
		fi

		echo "processing image file for iText hack also set background white"
		sh -c "$im $destfile -background white -alpha remove $destfile"

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
	rm -f $destthumbnail
	sh -c "ffmpeg -i $destfile -vf scale=-1:100  $destthumbnail"
fi

