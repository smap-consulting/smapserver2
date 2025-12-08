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
cp ~/deploy/fieldTaskPreJellyBean.apk ~/deploy/smap/deploy/version1
cp ~/deploy/smapUploader.jar ~/deploy/smap/deploy/version1
cp ~/deploy/codebook.jar ~/deploy/smap/deploy/version1

#
# sdDAL 
#
cd sdDAL
mvn clean install
cd ..

#
# sdDataAccess 
#
cd sdDataAccess
mvn clean install
cd ..

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
# subscribers runnable jar file
#
cd subscribers
ant -f subscriber3.xml
mv ~/deploy/subscribers.jar ~/deploy/smap/deploy/version1
mkdir ~/deploy/smap/deploy/version1/subscribers
cp -rf default ~/deploy/smap/deploy/version1/subscribers
cd ..

