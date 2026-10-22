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

# Fill in the host name the virtual hosts accept, from the "Host name" server setting.  The
# application uses the same setting for links that are emailed, so the two agree.  While it
# is not set the host check is removed, so a server without a domain name still works, as
# leaving the {hostname} placeholder in would refuse every request.
if [ -z "$DBHOST" ] && [ -f /etc/environment ]; then
    . /etc/environment
fi
host_sql="select hostname from server"
if [ -z "$DBHOST" ] || [ "$DBHOST" = "127.0.0.1" ]; then
    server_host=`sudo -i -u postgres psql -At -d survey_definitions -c "$host_sql" 2>/dev/null | head -1`
else
    server_host=`PGPASSWORD=keycar08 psql -h $DBHOST -U postgres -At -d survey_definitions -c "$host_sql" 2>/dev/null | head -1`
fi
if ! echo "$server_host" | grep -Eq '^[a-z0-9]([a-z0-9.-]*[a-z0-9])?(:[0-9]{1,5})?$'; then
    server_host=""
fi
for vhost in smap.conf smap-ssl.conf; do
    if [ -f $a_config_dir/$vhost ]; then
        if [ -n "$server_host" ]; then
            echo "Accepting requests for $server_host in $vhost"
            sudo sed -i "s/{hostname}/$server_host/g" $a_config_dir/$vhost
        else
            echo "No host name set in the server settings, $vhost will accept any host name"
            sudo sed -i "/req('Host') != '{hostname}'/,/<\/If>/d" $a_config_dir/$vhost
        fi
    fi
done

# Ensure apache loads the environment variables and export them
avars_set=`grep "\. /etc/environment" /etc/apache2/envvars | wc -l`
if [ $avars_set -eq 0 ]; then
    echo "set -a"  >> /etc/apache2/envvars
    echo ". /etc/environment" >> /etc/apache2/envvars
    echo "set +a"  >> /etc/apache2/envvars
fi
sudo service apache2 reload
