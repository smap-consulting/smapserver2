#!/bin/sh

# Set flag for ubuntu version
u1804=`lsb_release -r | grep -c "18\.04"`
u2004=`lsb_release -r | grep -c "20\.04"`
u2204=`lsb_release -r | grep -c "22\.04"`

if [ $u2204 -eq 1 ]; then
    TOMCAT_VERSION=tomcat9
    TOMCAT_USER=tomcat
elif [ $u2004 -eq 1 ]; then
    TOMCAT_VERSION=tomcat9
elif [ $u1804 -eq 1 ]; then
    TOMCAT_VERSION=tomcat8
else
    TOMCAT_VERSION=tomcat7
fi

echo "Updating Tomcat configuation"
tc_server_xml="/etc/$TOMCAT_VERSION/server.xml"	
sudo cp config_files/server.xml.$TOMCAT_VERSION $tc_server_xml

echo "Setting up Apache 2.4"
a_config_dir="/etc/apache2/sites-available"	

sudo cp $a_config_dir/smap-volatile.conf $a_config_dir/smap-volatile.conf.bu
sudo cp config_files/a24-smap-volatile.conf $a_config_dir/smap-volatile.conf

# Ensure apache loads the environment variables
avars_set=`grep "\. /etc/environment" /etc/apache2/envvars | wc -l`
if [ $avars_set -eq 0 ]; then
    echo ". /etc/environment" >> /etc/apache2/envvars
fi
sudo service apache2 reload

