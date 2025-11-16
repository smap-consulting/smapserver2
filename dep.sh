#!/bin/sh

#
# surveyMobileAPI war file
#
cd surveyMobileAPI
mvn clean install
cd ..
cp surveyMobileAPI/target/*.war ~/deploy/surveyMobileAPI.war

#
# koboTolboxApi war file
#
cd koboToolboxApi
mvn clean install
cd ..
cp koboToolboxApi/target/*.war ~/deploy/koboToolboxApi.war

#
# surveyKPI war file
#
cd surveyKPI
mvn clean install
cd ..
cp surveyKPI/target/*.war ~/deploy/surveyKPI.war

#
# subscribers runnable jar file
#
cd subscribers
ant -f subscriber3.xml
cd ..

#
# Install scripts
#
cd setup
./dep.sh
cd ..
