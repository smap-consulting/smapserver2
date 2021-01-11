#!/bin/sh

# Minify
#node tools/r.js -o tools/build.js
node tools/r_2_3_6.js -o tools/build.js

# Create a tar file and copy to the deploy directory
export COPYFILE_DISABLE=true
tar -zcf myWork.tgz myWork
cp myWork.tgz ~/deploy

# deploy to local
sudo rm -rf /Library/WebServer/Documents/app/myWork
sudo mkdir /Library/WebServer/Documents/app/myWork
sudo cp -rf myWork/* /Library/WebServer/Documents/app/myWork
sudo apachectl restart
rm myWork.tgz

# clean up the temporary myWork directory but first check that it is the right one
if [ -f dep.sh ]
then
	rm -rf myWork
fi
