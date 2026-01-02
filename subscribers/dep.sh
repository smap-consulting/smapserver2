#!/bin/sh

ant -f subscriber3.xml
mv ~/deploy/subscribers.jar ~/deploy/smap/deploy/version1
mkdir ~/deploy/smap/deploy/version1/subscribers
cp -rf default ~/deploy/smap/deploy/version1/subscribers
