#!/bin/sh
cd koboToolboxApi
mvn clean package
cp target/koboToolboxApi-1.0.0.war ~/deploy/koboToolboxApi.war
cd ..
