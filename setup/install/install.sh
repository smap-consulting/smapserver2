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

# Work out which Linux this is and check that its packages have the versions Smap needs,
# before anything on the server is changed
. ./platform.sh
platform_detect || exit 1

# The rest of this script uses apt, so for now only Debian and Ubuntu can be installed
if [ "$SMAP_FAMILY" != "debian" ]; then
    echo "ERROR: $SMAP_OS_NAME is not supported yet, only Debian and Ubuntu"
    exit 1
fi

# The database is remote if DBHOST is set to anything other than this server
dbhost_now=${DBHOST:-`sed -n 's/^DBHOST=//p' /etc/environment | tr -d '"' | tail -1`}
if [ -z "$dbhost_now" ] || [ "$dbhost_now" = "127.0.0.1" ]; then
    db_location=local
else
    db_location=remote
fi

pkg_update > /dev/null
platform_check_versions $db_location || exit 1
echo ""

# Add a default setting of 127.0.0.1 as the DBHOST if it has not already been set
dbhost_set=`grep DBHOST /etc/environment | wc -l`
if [ $dbhost_set -eq 0 ]; then
    echo "DBHOST=127.0.0.1" >> /etc/environment
    DBHOST=127.0.0.1; export DBHOST;
fi


TOMCAT_VERSION=tomcat10
TOMCAT_USER=tomcat

CATALINA_HOME=/var/lib/$TOMCAT_VERSION
sd="survey_definitions"											# Postgres config survey definitions db name
results="results"												# Postgres config results db name
tc_server_xml="/etc/$TOMCAT_VERSION/server.xml"					# Tomcat config
tc_context_xml="/etc/$TOMCAT_VERSION/context.xml"				# Tomcat config
tc_logging="/var/lib/$TOMCAT_VERSION/conf/logging.properties"	# Tomcat config
a_config_dir=$APACHE_SITE_DIR									# Apache config
a_config_conf="/etc/apache2/apache2.conf"						# Apache config
a_config_prefork_conf="/etc/apache2/mods-available/mpm_prefork.conf"		# Apache 2.4 config
a_default_xml="/etc/apache2/sites-available/default"						# Apache config
a_default_ssl_xml="/etc/apache2/sites-available/default-ssl"				# Apache config
service_dir="/etc/systemd/system"								# Subscriber config 

echo "Setting up your server to run Smap"
echo "If you have already installed Smap and just want to upgrade you need to run deploy.sh and not this script"
echo 'This script has been specified to work on Ubuntu only and assumes you are not using Apache or Postgres currently on the server'
echo 'However Smap can be deployed on any variant of Linux, Mac and Windows'
echo 'If you have an existing database then you may need to apply database patched to complete the installation. The script will not overwrite an existing database.  However you should BACK UP ANY EXISTING DATA BEFORE UPGRADING'

# The domain name, which can already exist or be created later, or the IP address if the server has
# no domain name.  It cannot be worked out from the machine, whose host name is usually an internal
# one.  Apache refuses requests for any other host name.  Set SMAP_DOMAIN to install without asking.
while ! echo "$SMAP_DOMAIN" | grep -Eq '^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$'
do
	read -r -p 'Domain name for this server, for example smap.example.com, or its IP address if it has none: ' SMAP_DOMAIN
done

read -r -p 'Do you want to continue? (y/n) ' choice
case $choice in
        n|N) break;;
        y|Y)

echo '##### 1. Update Ubuntu'
sudo apt-get update
sudo apt-get upgrade -y
sudo sysctl -w kernel.shmmax=67068800		# 64MB of shared memory
sudo apt-get install ntp -y
sudo apt-get install rename -y
sudo apt-get install locate -y

echo '##### 2. Install Apache' 
sudo apt-get install apache2 apache2-doc apache2-utils -y
sudo apt-get install libaprutil1-dbd-pgsql -y
sudo a2enmod auth_digest
sudo a2enmod expires
sudo a2enmod authn_dbd
sudo a2enmod proxy
sudo a2enmod proxy_ajp
sudo a2enmod ssl
sudo a2enmod headers

# Modules for module auth form
sudo a2enmod request
sudo a2enmod auth_form
sudo a2enmod session
sudo a2enmod session_cookie
sudo a2enmod session_crypto

sudo mkdir /var/www/smap

echo "##### 3. Install Tomcat: $TOMCAT_VERSION"
if [ "$TOMCAT_TARBALL" != "yes" ]; then
    # tomcat10 from the packages, which platform_check_versions found to be new enough
    sudo apt-get install openjdk-21-jdk-headless -y
    sudo apt-get install tomcat10 -y
    echo 'Fix tomcat10 logs symlink for systemd sandboxing'
    sudo systemctl stop tomcat10 2>/dev/null || true
    sudo mkdir -p /var/log/tomcat10
    sudo rm -rf /var/lib/tomcat10/logs
    sudo ln -s /var/log/tomcat10 /var/lib/tomcat10/logs
    sudo chown tomcat:tomcat /var/log/tomcat10
else
    # An Ubuntu LTS release whose packages do not have Tomcat 10.1: the tarball from apache.org
    tc_server_xml="/var/lib/$TOMCAT_VERSION/conf/server.xml"
    tc_context_xml="/var/lib/$TOMCAT_VERSION/conf/context.xml"
    tc_logging="/var/lib/$TOMCAT_VERSION/conf/logging.properties"

    echo 'install java 21'
    sudo apt-get install openjdk-21-jdk-headless -y
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
    JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
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

    sudo apt-get install postgresql postgresql-contrib postgis -y
    pg_conf="/etc/postgresql/$PGV/main/postgresql.conf"

else
    # Just install the psql client and create a postgres user
    sudo useradd -s /bin/sh -d /home/postgres -m postgres
    sudo apt-get install postgresql-client -y
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

sudo mkdir /var/lib/$TOMCAT_VERSION/.aws
sudo chown -R $TOMCAT_USER /var/lib/$TOMCAT_VERSION/.aws


# If auto configuration is set then copy the pre-set configuration files to their target destination

if [ "$config" != "manual" ]; then

	echo '##### 7. Copying configuration files'

	sudo service apache2 stop
	sudo service $TOMCAT_VERSION stop
        if [ "$DBHOST" = "127.0.0.1" ]; then
	    sudo service postgresql stop
	    # There is a configuration file only for the versions Smap has been installed with before
	    if [ -f config_files/postgresql.conf.$PGV ]; then
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

	echo '# copy Apache configuration file'
	if [ ! -f "$a_config_prefork_conf.bu" ]; then
		sudo mv $a_config_prefork_conf $a_config_prefork_conf.bu
	fi
	sudo cp config_files/mpm_prefork.conf $a_config_prefork_conf

	echo "Setting up Apache 2.4"
	if [ ! -f "$a_config_dir/smap.conf.bu" ]; then
		sudo cp $a_config_dir/smap.conf $a_config_dir/smap.conf.bu
	fi
	platform_template config_files/a24-smap.conf $a_config_dir/smap.conf

	if [ ! -f "$a_config_dir/smap-ssl.conf.bu" ]; then
		sudo cp $a_config_dir/smap-ssl.conf $a_config_dir/smap-ssl.conf.bu
	fi
	platform_template config_files/a24-smap-ssl.conf $a_config_dir/smap-ssl.conf
	ssl_ensure_self_signed $SMAP_DOMAIN

	# Fill in the domain name, or allow any host name if there is none
	chmod +x setDomain.sh
	sudo ./setDomain.sh $SMAP_DOMAIN
	
	# disable default config - TODO work out how to get Smap to coexist with existing Apache installations	
	sudo a2dissite 000-default
	sudo a2dissite default-ssl
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
    sudo service postgresql start
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

echo '##### 14. Add imagemagick,ffmpeg,exiftool to generate thumbnails'
if [ $(cat /etc/*-release | grep "DISTRIB_CODENAME=" | cut -d "=" -f2) = "trusty" ];
then
sudo add-apt-repository ppa:mc3man/trusty-media  && sudo apt-get update -y
fi

sudo apt-get install imagemagick -y
sudo apt-get install ffmpeg -y
sudo apt-get install libimage-exiftool-perl -y
#sudo apt-get install flvtool2 -y

if [ "$DBHOST" = "127.0.0.1" ]; then
    echo '##### 17. Backups'
    sudo mkdir ~postgres/backups
    sudo mkdir ~postgres/restore
    sudo chmod +x ~postgres/bu.sh ~postgres/re.sh
    sudo chown postgres ~postgres/bu.sh ~postgres/re.sh ~postgres/backups ~postgres/restore
fi

echo '##### 19. Update miscelaneous file configurations'

sudo apt-get install mlocate -y

echo '##### Add file location to tomcat configuration'

BU_FILE=/var/lib/$TOMCAT_VERSION/conf/web.xml.bu
if [ ! -f "$BU_FILE" ]; then
	sudo cp /var/lib/$TOMCAT_VERSION/conf/web.xml $BU_FILE
fi

sudo sed -i "/<\/web-app>/i \
<context-param>\n\
   <param-name>au.com.smap.files<\/param-name>\n\
   <param-value>$filelocn</param-value>\n\
<\/context-param>" /var/lib/$TOMCAT_VERSION/conf/web.xml

echo '##### Add shared memory setting to sysctl.conf'

if [ ! -f "/etc/sysctl.conf.bu" ]; then
	sudo cp /etc/sysctl.conf /etc/sysctl.conf.bu
fi
echo "kernel.shmmax=67068800" | sudo tee -a /etc/sysctl.conf 
# TODO add "-Djava.net.preferIPv4Stack=true" to JAVA_OPTS

echo '##### Increase shared memory available to tomcat'
if [ -f "/etc/default/$TOMCAT_VERSION" ]; then
	if [ ! -f "/etc/default/$TOMCAT_VERSION.bu" ]; then
		sudo cp /etc/default/$TOMCAT_VERSION /etc/default/$TOMCAT_VERSION.bu
	fi
	sudo sed -i "s#-Xmx128m#-Xmx512m#g" /etc/default/$TOMCAT_VERSION
fi

if [ "$DBHOST" = "127.0.0.1" ]; then
    echo '##### Allow logon to postgres authenticated by md5 - used to export shape files'
    # This could be better written as it is not idempotent, each time the install script is run an additional line will be changed
    if [ ! -f "/etc/postgresql/$PGV/main/pg_hba.conf.bu" ]; then
    	sudo mv /etc/postgresql/$PGV/main/pg_hba.conf /etc/postgresql/$PGV/main/pg_hba.conf.bu
    fi
    sudo awk 'BEGIN{doit=0;}/# "local"/{doit=1;isdone=0;}{if(doit==1){isdone=sub("peer","md5",$0);print;if(isdone==1){doit=0}}else{print}}' /etc/postgresql/$PGV/main/pg_hba.conf.bu > x
    sudo mv x /etc/postgresql/$PGV/main/pg_hba.conf
fi

echo '##### . Start the servers'
if [ "$DBHOST" = "127.0.0.1" ]; then
    sudo service postgresql start
fi
# Copy PostgreSQL JDBC driver to Tomcat shared lib before first startup
cp ../deploy/version1/jdbc/* /var/lib/$TOMCAT_VERSION/lib/
sudo service $TOMCAT_VERSION start
sudo service apache2 start

echo '##### 20. Enable export to shape files, kmz files and pdf files'
sudo apt-get install zip -y
sudo apt-get install gdal-bin -y
sudo apt-get install ttf-dejavu -y

# Add a file containing the version number
echo "2608" > /smap_bin/smap_version

echo '##### 21. Add postgres and apache to tomcat group'
if [ "$DBHOST" = "127.0.0.1" ]; then
    sudo usermod -a -G $TOMCAT_USER postgres
fi
sudo usermod -a -G $TOMCAT_USER www-data

echo '##### 22. Deploy Smap'
cd ../deploy
chmod +x patchdb.sh
sudo ./patchdb.sh
chmod +x deploy.sh
sudo ./deploy.sh
cd ../install

esac

