#!/bin/sh

echo "================================================="
echo "processing $0 $1 $2" 

in=$1
out=$2

echo "--------------------------------------"
echo "converting audio"
sh -c "/usr/bin/ffmpeg -y -i $in -ar 22050  $out"
