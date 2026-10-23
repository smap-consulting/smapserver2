#!/bin/sh

# Where this server's components are, saved by install.sh or deploy.sh
. ./platform.sh
platform_detect || exit 1
[ -f $SMAP_PLATFORM_CONF ] && . $SMAP_PLATFORM_CONF

TOMCAT_VERSION=tomcat10
tc_server_xml="$TOMCAT_CONF_DIR/server.xml"

echo "Updating Tomcat configuration"
sudo cp config_files/server.xml.$TOMCAT_VERSION $tc_server_xml

echo "Setting up Apache 2.4"
a_config_dir=$APACHE_SITE_DIR

[ -f $a_config_dir/smap-volatile.conf ] && sudo cp $a_config_dir/smap-volatile.conf $a_config_dir/smap-volatile.conf.bu
sudo cp config_files/a24-smap-volatile.conf $a_config_dir/smap-volatile.conf

# Ensure apache loads the environment variables and export them
if [ $SMAP_FAMILY = "debian" ]; then
    avars_set=`grep "\. /etc/environment" /etc/apache2/envvars | wc -l`
    if [ $avars_set -eq 0 ]; then
        echo "set -a"  >> /etc/apache2/envvars
        echo ". /etc/environment" >> /etc/apache2/envvars
        echo "set +a"  >> /etc/apache2/envvars
    fi
else
    # There is no envvars file, systemd passes them to Apache instead
    mkdir -p /etc/systemd/system/$APACHE_SERVICE.service.d
    printf '[Service]\nEnvironmentFile=/etc/environment\n' > /etc/systemd/system/$APACHE_SERVICE.service.d/smap-environment.conf
    systemctl daemon-reload
fi
sudo systemctl reload $APACHE_SERVICE
