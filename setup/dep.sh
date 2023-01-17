#!/bin/sh

# Delete existing setting deployment directory
rm -rf ~/deploy/smap
mkdir ~/deploy/smap

# Get files
cp -rf deploy ~/deploy/smap
cp -rf install ~/deploy/smap

