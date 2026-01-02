#!/bin/sh
mvn clean install
cp target/*.war ~/deploy/smap/deploy/version1/surveyMobileAPI.war
