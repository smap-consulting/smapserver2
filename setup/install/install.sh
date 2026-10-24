#!/bin/sh
##### Installation script for setting up basic Smap server without ssl or virtual hosts

force=$1

if [ "$force" = "force" ]
then
	echo "All existing data will be deleted"
fi

config="auto"
clean="true"
filelocn="/smap"

if [ "`id -u`" != "0" ]; then
    echo "ERROR: Run the install as root, for example: sudo ./install.sh"
    exit 1
fi

# Work out which Linux this is and check that its packages have the versions Smap needs,
# before anything on the server is changed
. ./platform.sh
platform_detect || exit 1

# The database is remote if DBHOST is set to anything other than this server
dbhost_now=${DBHOST:-`sed -n 's/^DBHOST=//p' /etc/environment | tr -d '"' | tail -1`}
if [ -z "$dbhost_now" ] || [ "$dbhost_now" = "127.0.0.1" ]; then
    db_location=local
else
    db_location=remote
fi

pkg_update > /dev/null
if [ $db_location = "local" ]; then
    platform_add_required_repos
fi
platform_check_versions $db_location || exit 1
echo ""

# Add a default setting of 127.0.0.1 as the DBHOST if it has not already been set
dbhost_set=`grep DBHOST /etc/environment | wc -l`
if [ $dbhost_set -eq 0 ]; then
    echo "DBHOST=127.0.0.1" >> /etc/environment
fi
# sudo does not pass on /etc/environment on all distributions, Debian 13 for one
DBHOST=${dbhost_now:-127.0.0.1}; export DBHOST


TOMCAT_VERSION=tomcat10

sd="survey_definitions"											# Postgres config survey definitions db name
results="results"												# Postgres config results db name
tc_server_xml="$TOMCAT_CONF_DIR/server.xml"						# Tomcat config
tc_context_xml="$TOMCAT_CONF_DIR/context.xml"					# Tomcat config
tc_logging="$TOMCAT_CONF_DIR/logging.properties"				# Tomcat config
a_config_dir=$APACHE_SITE_DIR									# Apache config
a_config_prefork_conf="/etc/apache2/mods-available/mpm_prefork.conf"		# Apache 2.4 config, Debian
service_dir="/etc/systemd/system"								# Subscriber config

echo "Setting up your server to run Smap"
echo "If you have already installed Smap and just want to upgrade you need to run deploy.sh and not this script"
echo 'This script assumes you are not using Apache, Tomcat or Postgres currently on the server'
echo 'If you have an existing database then you may need to apply database patched to complete the installation. The script will not overwrite an existing database.  However you should BACK UP ANY EXISTING DATA BEFORE UPGRADING'

# The domain name, which can already exist or be created later, or the IP address if the server has
# no domain name.  It cannot be worked out from the machine, whose host name is usually an internal
# one.  Apache refuses requests for any other host name.  Set SMAP_DOMAIN to install without asking.
while ! echo "$SMAP_DOMAIN" | grep -Eq '^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$'
do
	printf 'Domain name for this server, for example smap.example.com, or its IP address if it has none: '
	read -r SMAP_DOMAIN
done

printf 'Do you want to continue? (y/n) '
read -r choice
case $choice in
        n|N) break;;
        y|Y)

echo "##### 1. Update $SMAP_OS_NAME"
if [ $SMAP_FAMILY = "debian" ]; then
    sudo apt-get update
    sudo apt-get upgrade -y
    # Debian 13 has no ntp, systemd-timesyncd keeps the time instead
    [ -n "`pkg_candidate ntp`" ] && sudo apt-get install ntp -y
    sudo apt-get install rename -y
    sudo apt-get install locate -y
fi
command -v sudo > /dev/null || pkg_install sudo		# The deploy scripts use it
sudo sysctl -w kernel.shmmax=67068800		# 64MB of shared memory

echo '##### 2. Install Apache'
pkg_install $PKG_APACHE
for m in $SMAP_APACHE_MODULES
do
    apache_enable_module $m
done
apache_enable_https
systemctl enable $APACHE_SERVICE
mkdir -p $APACHE_SITE_DIR		# Only Debian has one already

sudo mkdir /var/www/smap

echo "##### 3. Install Tomcat"
if [ "$TOMCAT_TARBALL" != "yes" ]; then
    # Tomcat from the packages, which platform_check_versions found to be new enough
    pkg_install $PKG_JAVA $PKG_TOMCAT
    if [ $SMAP_FAMILY = "debian" ]; then
        echo 'Fix tomcat10 logs symlink for systemd sandboxing'
        sudo systemctl stop tomcat10 2>/dev/null || true
        sudo mkdir -p /var/log/tomcat10
        sudo rm -rf /var/lib/tomcat10/logs
        sudo ln -s /var/log/tomcat10 /var/lib/tomcat10/logs
        sudo chown tomcat:tomcat /var/log/tomcat10
    fi
    systemctl enable $TOMCAT_SERVICE
else
    # An Ubuntu LTS release whose packages do not have Tomcat 10.1: the tarball from apache.org
    echo 'install java 21'
    pkg_install $PKG_JAVA
    echo 'Create tomcat user'
    sudo groupadd tomcat 2>/dev/null || true
    sudo useradd -s /bin/false -g tomcat -d /var/lib/tomcat10 tomcat 2>/dev/null || true
    echo 'get tomcat'
    TC10_VER=$SMAP_TOMCAT_TARBALL_VERSION
    wget https://archive.apache.org/dist/tomcat/tomcat-10/v${TC10_VER}/bin/apache-tomcat-${TC10_VER}.tar.gz
    sudo mkdir -p /var/lib/tomcat10
    sudo tar xzf apache-tomcat-10*tar.gz -C /var/lib/tomcat10 --strip-components=1
    rm apache-tomcat-10*tar.gz
    echo 'Tomcat service'
    cp config_files/tomcat10.service /usr/lib/systemd/system
    JAVA_HOME=`java_home`
    JH="Environment=\"JAVA_HOME=$JAVA_HOME\""
    sed -i "/JAVA_HOME/c$JH" /usr/lib/systemd/system/tomcat10.service
    echo 'Create tomcat log directory'
    mkdir -p /var/log/tomcat10
    rm -rf /var/lib/tomcat10/logs
    ln -s /var/log/tomcat10 /var/lib/tomcat10/logs
    chown -R tomcat /var/lib/tomcat10 /var/log/tomcat10
    chgrp -R tomcat /var/lib/tomcat10 /var/log/tomcat10
    systemctl enable tomcat10
fi

echo '##### 5. Install Postgres / Postgis'

# Skip this section if the database is remote
if [ "$DBHOST" = "127.0.0.1" ]; then

    echo 'installing postgres'
    # The major version the packages provide, found by platform_check_versions
    PGV=$SMAP_PG_VERSION

    pkg_install $PKG_PG_SERVER `pkg_postgis $PGV`
    pg_paths $PGV
    pg_initdb
    pg_pin
    pg_login_shell
    systemctl enable $PG_SERVICE
    pg_conf="$PG_CONF_DIR/postgresql.conf"

else
    # Just install the psql client and create a postgres user
    id postgres > /dev/null 2>&1 || sudo useradd -s /bin/sh -d /home/postgres -m postgres
    pkg_install $PKG_PG_CLIENT
fi
# End of conditional install

# Save where everything was installed, for apacheConfig.sh, patchdb.sh and deploy.sh
pg_paths $SMAP_PG_VERSION
platform_validate || exit 1
platform_save

echo "##### 6. Create folders for files in $filelocn"
sudo mkdir $filelocn
sudo mkdir $filelocn/attachments
sudo mkdir $filelocn/attachments/report
sudo mkdir $filelocn/attachments/report/thumbs
sudo mkdir $filelocn/media
sudo mkdir $filelocn/media/organisation
sudo mkdir $filelocn/templates
sudo mkdir $filelocn/uploadedSurveys
sudo mkdir $filelocn/misc
sudo mkdir $filelocn/temp
sudo mkdir $filelocn/settings

# Allow tomcat to write to /smap (systemd override)
mkdir -p /etc/systemd/system/$TOMCAT_SERVICE.service.d
cp config_files/override.conf /etc/systemd/system/$TOMCAT_SERVICE.service.d/override.conf

# Make sure all subdirectories of filelocn are updated even if the latter is a symbolic link
sudo chown -R $TOMCAT_USER $filelocn
sudo chown -R $TOMCAT_USER $filelocn/*
sudo chmod -R 0777 $filelocn/*

sudo mkdir $TOMCAT_BASE_DIR/.aws
sudo chown -R $TOMCAT_USER $TOMCAT_BASE_DIR/.aws


# If auto configuration is set then copy the pre-set configuration files to their target destination

if [ "$config" != "manual" ]; then

	echo '##### 7. Copying configuration files'

	sudo systemctl stop $APACHE_SERVICE
	sudo systemctl stop $TOMCAT_SERVICE
        if [ "$DBHOST" = "127.0.0.1" ]; then
	    sudo systemctl stop $PG_SERVICE
	    # There is a configuration file only for the Debian and Ubuntu versions Smap has been installed with before
	    if [ $SMAP_FAMILY = "debian" ] && [ -f config_files/postgresql.conf.$PGV ]; then
	        echo '# copy postgres conf file'
		    if [ ! -f "$pg_conf.bu" ]; then
	    	    sudo mv $pg_conf $pg_conf.bu
	        fi
	        sudo cp config_files/postgresql.conf.$PGV $pg_conf
	    else
	        echo "# no postgres conf file for version $PGV, keeping the one installed with it"
	    fi
	fi

	echo '# copy tomcat server file'
	if [ ! -f "$tc_server_xml.bu" ]; then
		sudo mv $tc_server_xml $tc_server_xml.bu
	fi
	sudo cp config_files/server.xml.$TOMCAT_VERSION $tc_server_xml
	sudo chown $TOMCAT_USER $tc_server_xml

	echo '# copy tomcat context file'
	if [ ! -f "$tc_context_xml.bu" ]; then
		sudo mv $tc_context_xml $tc_context_xml.bu
	fi
	sudo cp config_files/context.xml $tc_context_xml
	sudo chown $TOMCAT_USER $tc_context_xml

	echo '# copy tomcat logging properties file'
	if [ ! -f "$tc_logging.bu" ]; then
		sudo mv $tc_logging $tc_logging.bu
	fi
	sudo cp config_files/logging.properties $tc_logging

	if [ $SMAP_FAMILY = "debian" ]; then
		echo '# copy Apache configuration file'
		if [ ! -f "$a_config_prefork_conf.bu" ]; then
			sudo mv $a_config_prefork_conf $a_config_prefork_conf.bu
		fi
		sudo cp config_files/mpm_prefork.conf $a_config_prefork_conf
	fi

	echo "Setting up Apache 2.4"
	if [ ! -f "$a_config_dir/smap.conf.bu" ] && [ -f "$a_config_dir/smap.conf" ]; then
		sudo cp $a_config_dir/smap.conf $a_config_dir/smap.conf.bu
	fi
	platform_template config_files/a24-smap.conf $a_config_dir/smap.conf

	if [ ! -f "$a_config_dir/smap-ssl.conf.bu" ] && [ -f "$a_config_dir/smap-ssl.conf" ]; then
		sudo cp $a_config_dir/smap-ssl.conf $a_config_dir/smap-ssl.conf.bu
	fi
	platform_template config_files/a24-smap-ssl.conf $a_config_dir/smap-ssl.conf
	ssl_ensure_self_signed $SMAP_DOMAIN

	# Fill in the domain name, or allow any host name if there is none
	chmod +x setDomain.sh
	sudo ./setDomain.sh $SMAP_DOMAIN

	# disable default config - TODO work out how to get Smap to coexist with existing Apache installations
	if [ $SMAP_FAMILY = "debian" ]; then
		sudo a2dissite 000-default
		sudo a2dissite default-ssl
	fi
	apache_enable_site smap.conf
	apache_enable_site smap-ssl.conf

	# Update the volatile configuration setting, only this should change after initial installation
	chmod +x apacheConfig.sh
	sudo ./apacheConfig.sh

	echo '# copy subscriber service files'

	platform_template config_files/subscribers.service $service_dir/subscribers.service
	sudo chmod 664 $service_dir/subscribers.service
	platform_template config_files/subscribers_fwd.service $service_dir/subscribers_fwd.service
	sudo chmod 664 $service_dir/subscribers_fwd.service

	sudo systemctl enable subscribers.service
	sudo systemctl enable subscribers_fwd.service

    if [ "$DBHOST" = "127.0.0.1" ]; then
	    echo '# update bu.sh file'
	    sudo cp bu.sh ~postgres/bu.sh
	    sudo chown postgres ~postgres/bu.sh

	    echo '# update re.sh file'
	    sudo cp re.sh ~postgres/re.sh
	    sudo chown postgres ~postgres/re.sh
	fi

else
	echo '##### 7. Skipping auto configuration'

fi

if [ "$DBHOST" = "127.0.0.1" ]; then

    echo '##### 9. Create user and databases'
    sudo systemctl start $PG_SERVICE
    sudo -i -u postgres createuser -S -D -R ws
    echo "alter user ws with password 'ws1234'" | sudo -i -u postgres psql

    echo '##### 10. Create $sd database'

    if [ "$force" = "force" ]
    then
	echo "drop database $sd;" | sudo -i -u postgres psql
    fi

    sd_exists=`sudo -i -u postgres psql -lqt | cut -d \| -f 1 | grep -w $sd | wc -l`
    if [ "$sd_exists"  = "0" ]
    then
        echo 'survey_definitions database does not exist'
        sudo -i -u postgres createdb -E UTF8 -O ws $sd
        echo "CREATE EXTENSION postgis;" | sudo -i -u postgres psql -d $sd
        echo "CREATE EXTENSION pgcrypto;" | sudo -i -u postgres psql -d $sd
        echo "ALTER TABLE geometry_columns OWNER TO ws; ALTER TABLE spatial_ref_sys OWNER TO ws; ALTER TABLE geography_columns OWNER TO ws;" | sudo -i -u postgres psql -d $sd
        cat setupDb.sql | sudo -i -u postgres psql -d $sd | grep -v "skipping"
    else
        echo "==================> $sd database already exists.  Apply patches if necessary, to upgrade it."
    fi

    echo '##### 11. Create $results database'

    if [ "$force" = "force" ]
    then
	echo "drop database $results;" | sudo -i -u postgres psql
    fi

    results_exists=`sudo -i -u postgres psql -lqt | cut -d \| -f 1 | grep -w $results | wc -l`
    if [ "$results_exists"  = "0" ]
    then
        echo 'results database does not exist'
        sudo -i -u postgres createdb -E UTF8 -O ws $results
        echo "CREATE EXTENSION postgis;" | sudo -u postgres psql -d $results
        echo "CREATE EXTENSION pgcrypto;" | sudo -u postgres psql -d $results
        sudo -i -u postgres echo "ALTER TABLE geometry_columns OWNER TO ws; ALTER TABLE spatial_ref_sys OWNER TO ws; ALTER TABLE geography_columns OWNER TO ws;" | sudo -i -u postgres psql -d $results
        cat resultsDb.sql | sudo -i -u postgres psql -d $results

    else
        echo "==================> $results database already exists.  Apply patches if necessary, to upgrade it."
    fi
fi

echo '##### 12. Setup subscribers'
sudo rm -rf /smap_bin
sudo mkdir /smap_bin
sudo mkdir /var/log/subscribers
sudo cp subscribers.sh /smap_bin
sudo chmod -R 777 /var/log/subscribers
sudo chmod -R +x /var/log/subscribers
sudo chmod +x /smap_bin/subscribers.sh
sudo mkdir /smap_bin/resources
sudo mkdir /smap_bin/resources/css


echo '##### 13. Set up deployment script'
chmod +x ../deploy/deploy.sh

echo '##### 14. Add imagemagick,ffmpeg,exiftool to generate thumbnails, gdal for shape files, zip and fonts'
platform_offer_media_repos
# Only those the packages have, the version check has already warned about any that are missing
optional=""
for p in $PKG_MEDIA $PKG_UTILS
do
    [ -n "`pkg_candidate $p`" ] && optional="$optional $p"
done
[ -n "$optional" ] && pkg_install $optional

if [ "$DBHOST" = "127.0.0.1" ]; then
    echo '##### 17. Backups'
    sudo mkdir ~postgres/backups
    sudo mkdir ~postgres/restore
    sudo chmod +x ~postgres/bu.sh ~postgres/re.sh
    sudo chown postgres ~postgres/bu.sh ~postgres/re.sh ~postgres/backups ~postgres/restore
fi

echo '##### 19. Update miscelaneous file configurations'

echo '##### Add file location to tomcat configuration'

BU_FILE=$TOMCAT_CONF_DIR/web.xml.bu
if [ ! -f "$BU_FILE" ]; then
	sudo cp $TOMCAT_CONF_DIR/web.xml $BU_FILE
fi

sudo sed -i "/<\/web-app>/i \
<context-param>\n\
   <param-name>au.com.smap.files<\/param-name>\n\
   <param-value>$filelocn</param-value>\n\
<\/context-param>" $TOMCAT_CONF_DIR/web.xml

echo '##### Add shared memory setting to sysctl.conf'

if [ ! -f "/etc/sysctl.conf.bu" ] && [ -f /etc/sysctl.conf ]; then
	sudo cp /etc/sysctl.conf /etc/sysctl.conf.bu
fi
echo "kernel.shmmax=67068800" | sudo tee -a /etc/sysctl.conf
# TODO add "-Djava.net.preferIPv4Stack=true" to JAVA_OPTS

echo '##### Increase shared memory available to tomcat'
if [ -f "/etc/default/$TOMCAT_SERVICE" ]; then
	if [ ! -f "/etc/default/$TOMCAT_SERVICE.bu" ]; then
		sudo cp /etc/default/$TOMCAT_SERVICE /etc/default/$TOMCAT_SERVICE.bu
	fi
	sudo sed -i "s#-Xmx128m#-Xmx512m#g" /etc/default/$TOMCAT_SERVICE
fi

if [ "$DBHOST" = "127.0.0.1" ]; then
    echo '##### Allow logon to postgres authenticated by md5 - used to export shape files'
    pg_configure_logins
fi

platform_selinux

echo '##### . Start the servers'
if [ "$DBHOST" = "127.0.0.1" ]; then
    sudo systemctl start $PG_SERVICE
fi
# Copy PostgreSQL JDBC driver to Tomcat shared lib before first startup
cp ../deploy/version1/jdbc/* $TOMCAT_LIB/
sudo systemctl start $TOMCAT_SERVICE
sudo systemctl start $APACHE_SERVICE

# Add a file containing the version number
echo "2608" > /smap_bin/smap_version

echo '##### 21. Add postgres and apache to tomcat group'
if [ "$DBHOST" = "127.0.0.1" ]; then
    sudo usermod -a -G $TOMCAT_USER postgres
fi
sudo usermod -a -G $TOMCAT_USER $APACHE_USER

echo '##### 22. Deploy Smap'
cd ../deploy
chmod +x patchdb.sh
sudo ./patchdb.sh
chmod +x deploy.sh
sudo ./deploy.sh
cd ../install

esac

