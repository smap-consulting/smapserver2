#!/bin/sh

#
# Install scripts
#
cd setup
./dep.sh
cd ..

#
# Miscelaneous files
#
cp ~/deploy/fieldTask.apk ~/deploy/smap/deploy/version1
cp ~/deploy/meqa.apk ~/deploy/smap/deploy/version1
cp ~/deploy/fieldTaskPreJellyBean.apk ~/deploy/smap/deploy/version1
cp ~/deploy/fpReader2.apk ~/deploy/smap/deploy/version1
cp ~/deploy/smapUploader.jar ~/deploy/smap/deploy/version1

#
# amazon
#
# amazon - must be built before sdDAL to keep ~/.m2 jar in sync
#
cd amazon
mvn clean install
cd ..

#
# sdDAL
#
cd sdDAL
mvn clean install
cd ..

#
# codebook jar
#
cd codebook
mvn clean install
cd ..
cp codebook/target/codebook.jar ~/deploy/smap/deploy/version1

#
# surveyMobileAPI war file
#
cd surveyMobileAPI
mvn clean install
cd ..
cp surveyMobileAPI/target/*.war ~/deploy/smap/deploy/version1/surveyMobileAPI.war

#
# koboTolboxApi war file
#
cd koboToolboxApi
mvn clean install
cd ..
cp koboToolboxApi/target/*.war ~/deploy/smap/deploy/version1/koboToolboxApi.war

#
# surveyKPI war file
#
cd surveyKPI
mvn clean install
cd ..
cp surveyKPI/target/*.war ~/deploy/smap/deploy/version1/surveyKPI.war

#
# subscribers jar file.  Not runnable on its own, it is started with -cp against the
# shared libraries below - see setup/install/subscribers.sh
#
cd subscribers
mvn clean package
cp target/subscribers.jar ~/deploy/smap/deploy/version1/subscribers.jar
mkdir -p ~/deploy/smap/deploy/version1/subscribers
cp -rf default ~/deploy/smap/deploy/version1/subscribers
cd ..

#
# Shared libraries
#
# The war files and subscribers.jar exclude the libraries listed in shared-libs.txt, they
# are deployed once to /smap_bin/lib instead so that there is one copy rather than one per
# war plus another in the subscribers jar.  surveyKPI has the largest dependency set so
# the jars are taken from there.
#
# Runs last so that it can check the war files and the subscribers jar that were just
# built for libraries that should have been left out of them.
#
./shared_libs.sh

