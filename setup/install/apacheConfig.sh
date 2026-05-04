#!/bin/sh

# Set flag for ubuntu version
u2204=`lsb_release -r | grep -c "22\.04"`
u2404=`lsb_release -r | grep -c "24\.04"`
u2604=`lsb_release -r | grep -c "26\.04"`

TOMCAT_VERSION=tomcat10
TOMCAT_USER=tomcat
if [ $u2604 -eq 1 ]; then
    # 26.04: apt install - config at /etc/tomcat10
    tc_server_xml="/etc/$TOMCAT_VERSION/server.xml"
else
    # 24.04 and 22.04: manual install - config at /var/lib/tomcat10/conf
    tc_server_xml="/var/lib/$TOMCAT_VERSION/conf/server.xml"
fi

echo "Updating Tomcat configuration"
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
