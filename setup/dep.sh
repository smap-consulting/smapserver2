#!/bin/sh

# Delete existing setting deployment directory
rm -rf ~/deploy/smap
mkdir ~/deploy/smap

# Get files
cp -rf ~/git/smapserver2/setup/deploy/* ~/deploy/smap/deploy
cp -rf ~/git/smapserver2/setup/install/* ~/deploy/smap/install

