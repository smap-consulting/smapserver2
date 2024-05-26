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

# Add a default setting of 127.0.0.1 as the DBHOST if it has not already been set
dbhost_set=`grep DBHOST /etc/environment | wc -l`
if [ $dbhost_set -eq 0 ]; then
    echo "export DBHOST=127.0.0.1" >> /etc/environment
    DBHOST=127.0.0.1; export DBHOST;
fi

# Set flag for ubuntu version
u1804=`lsb_release -r | grep -c "18\.04"`
u2004=`lsb_release -r | grep -c "20\.04"`
u2204=`lsb_release -r | grep -c "22\.04"`
u2404=`lsb_release -r | grep -c "24\.04"`

# Check that this version of ubuntu is supported
if [ $u2404 -eq 1 ]; then
    echo "Installing on Ubuntu 24.04"
elif [ $u2204 -eq 1 ]; then
    echo "Installing on Ubuntu 22.04"
elif [ $u2004 -eq 1 ]; then
    echo "Installing on Ubuntu 20.04"
elif [ $u1804 -eq 1 ]; then
    echo "Installing on Ubuntu 18.04"
else
    echo "Unsupported version of Ubuntu, you need 22.04, 20.04, or 18.04"
    exit 1;
fi

if [ $u2404 -eq 1 ]; then
    TOMCAT_VERSION=tomcat9
    TOMCAT_USER=tomcat
elif [ $u2204 -eq 1 ]; then
    TOMCAT_VERSION=tomcat9
    TOMCAT_USER=tomcat
elif [ $u2004 -eq 1 ]; then
    TOMCAT_VERSION=tomcat9
    TOMCAT_USER=tomcat
else
    TOMCAT_VERSION=tomcat8
    TOMCAT_USER=tomcat8
fi

CATALINA_HOME=/usr/share/$TOMCAT_VERSION
sd="survey_definitions"											# Postgres config survey definitions db name
results="results"												# Postgres config results db name
tc_server_xml="/etc/$TOMCAT_VERSION/server.xml"					# Tomcat config
tc_context_xml="/etc/$TOMCAT_VERSION/context.xml"				# Tomcat config
tc_logging="/var/lib/$TOMCAT_VERSION/conf/logging.properties"	# Tomcat config
a_config_dir="/etc/apache2/sites-available"						# Apache config	
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
if [ $u2404 -eq 1 ]; then

    tc_server_xml="/var/lib/$TOMCAT_VERSION/conf/server.xml"					# Tomcat config
    tc_context_xml="/var/lib/$TOMCAT_VERSION/conf/context.xml"				# Tomcat config

    echo 'install java 11'
    sudo apt-get install openjdk-11-jre-headless -y
    echo 'Create tomcat user'
    sudo groupadd tomcat
    sudo useradd -s /bin/false -g tomcat -d /usr/share/tomcat9 tomcat
    echo 'get tomcat'
    wget https://dlcdn.apache.org/tomcat/tomcat-9/v9.0.89/bin/apache-tomcat-9.0.89.tar.gz
    sudo mkdir /usr/share/tomcat9
    sudo tar xzf apache-tomcat-9*tar.gz -C /usr/share/tomcat9 --strip-components=1
    rm apache-tomcat-9*tar.gz
    echo 'Tomcat service'
    cp config_files/tomcat9.service /usr/lib/systemd/system
    echo 'Create tomcat apps directory'
    mkdir /var/lib/tomcat9
    mkdir /var/lib/tomcat9/webapps
    mkdir /var/lib/tomcat9/conf
    mkdir /var/lib/tomcat9/logs
    echo 'Create tomcat log directory'
    mkdir /var/log/tomcat9
    chown -R tomcat /var/lib/tomcat9 /var/log/tomcat9 /usr/share/tomcat9
    chgrp -R tomcat /var/lib/tomcat9 /var/log/tomcat9 /usr/share/tomcat9
    systemctl enable tomcat9
else
    sudo apt-get install $TOMCAT_VERSION -y
fi

echo '##### 5. Install Postgres / Postgis'

# Skip this section if the database is remote
if [ "$DBHOST" = "127.0.0.1" ]; then

    echo 'installing postgres'
    # Install Postgres for Ubuntu 24.04
    if [ $u2404 -eq 1 ]; then
        PGV=16
    fi

    # Install Postgres for Ubuntu 22.04
    if [ $u2204 -eq 1 ]; then
        PGV=14
    fi

    # Install Postgres for Ubuntu 20.04
    if [ $u2004 -eq 1 ]; then
        PGV=12
    fi

    # Install Postgres for Ubuntu 18.04
    if [ $u1804 -eq 1 ]; then
        PGV=10
    fi

    sudo apt-get install postgresql postgresql-contrib postgis -y
    pg_conf="/etc/postgresql/$PGV/main/postgresql.conf"

else
    # Just install the psql client and create a postgres user
    sudo useradd -s /bin/sh -d /home/postgres -m postgres
    sudo apt-get install postgresql-client
fi
# End of conditional install

echo "##### 6. Create folders for files in $filelocn"
sudo mkdir $filelocn
sudo mkdir $filelocn/attachments
sudo mkdir $filelocn/attachments/report
sudo mkdir $filelocn/attachments/report/thumbs
sudo mkdir $filelocn/media
sudo mkdir $filelocn/media/organisation
sudo mkdir $filelocn/templates
sudo mkdir $filelocn/templates/xls
sudo mkdir $filelocn/uploadedSurveys
sudo mkdir $filelocn/misc
sudo mkdir $filelocn/temp
sudo mkdir $filelocn/settings

# For ubuntu 2404 allow tomcat9 to write to /smap
if [ $u2404 -eq 1 ]; then
mkdir /etc/systemd/system/tomcat9.service.d
cp config_files/override.conf /etc/systemd/system/tomcat9.service.d/override.conf
fi

# For ubuntu 2204 allow tomcat9 to write to /smap
if [ $u2204 -eq 1 ]; then
mkdir /etc/systemd/system/tomcat9.service.d
cp config_files/override.conf /etc/systemd/system/tomcat9.service.d/override.conf
fi

# For ubuntu 2004 allow tomcat9 to write to /smap
if [ $u2004 -eq 1 ]; then
mkdir /etc/systemd/system/tomcat9.service.d
cp config_files/override.conf /etc/systemd/system/tomcat9.service.d/override.conf
fi

# Make sure all subdirectories of filelocn are updated even if the latter is a symbolic link
sudo chown -R $TOMCAT_USER $filelocn
sudo chown -R $TOMCAT_USER $filelocn/*
sudo chmod -R 0777 $filelocn/*

if [ $u2004 -eq 1 ]; then
    sudo mkdir /var/lib/$TOMCAT_VERSION/.aws
    sudo chown -R $TOMCAT_USER /var/lib/$TOMCAT_VERSION/.aws
elif [ $u1804 -eq 1 ]; then
    sudo mkdir /var/lib/$TOMCAT_VERSION/.aws
    sudo chown -R $TOMCAT_USER /var/lib/$TOMCAT_VERSION/.aws
else
    sudo mkdir /usr/share/$TOMCAT_VERSION/.aws
    sudo chown -R $TOMCAT_USER /usr/share/$TOMCAT_VERSION/.aws
fi

# If auto configuration is set then copy the pre-set configuration files to their target destination

if [ "$config" != "manual" ]
then
	echo '##### 7. Copying configuration files'

	sudo service apache2 stop
	sudo service $TOMCAT_VERSION stop
        if [ $DBHOST = "127.0.0.1" ]; then
	    sudo service postgresql stop
	    echo '# copy postgres conf file'
		if [ ! -f "$pg_conf.bu" ]; then
	    	sudo mv $pg_conf $pg_conf.bu
	    fi
	    sudo cp config_files/postgresql.conf.$PGV $pg_conf
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
	sudo cp config_files/a24-smap.conf $a_config_dir/smap.conf

	if [ ! -f "$a_config_dir/smap-ssl.conf.bu" ]; then
		sudo cp $a_config_dir/smap-ssl.conf $a_config_dir/smap-ssl.conf.bu
	fi
	sudo cp config_files/a24-smap-ssl.conf $a_config_dir/smap-ssl.conf
	
	# disable default config - TODO work out how to get Smap to coexist with existing Apache installations	
	sudo a2dissite 000-default
	sudo a2dissite default-ssl
	sudo a2ensite smap.conf
	sudo a2ensite smap-ssl.conf
	
	# Update the volatile configuration setting, only this should change after initial installation
	chmod +x apacheConfig.sh
	sudo ./apacheConfig.sh

	echo '# copy subscriber upstart files'
	if [ $u2404 -eq 1 ]; then
		sudo cp config_files/subscribers.service.u2004 $service_dir/subscribers.service
		sudo chmod 664 $service_dir/subscribers.service
		sudo cp config_files/subscribers_fwd.service.u2004 $service_dir/subscribers_fwd.service
		sudo chmod 664 $service_dir/subscribers_fwd.service
		
		sudo systemctl enable subscribers.service
		sudo systemctl enable subscribers_fwd.service
	fi

	if [ $u2204 -eq 1 ]; then
		sudo cp config_files/subscribers.service.u2004 $service_dir/subscribers.service
		sudo chmod 664 $service_dir/subscribers.service
		sudo cp config_files/subscribers_fwd.service.u2004 $service_dir/subscribers_fwd.service
		sudo chmod 664 $service_dir/subscribers_fwd.service
		
		sudo systemctl enable subscribers.service
		sudo systemctl enable subscribers_fwd.service
	fi

	if [ $u2004 -eq 1 ]; then
		sudo cp config_files/subscribers.service.u2004 $service_dir/subscribers.service
		sudo chmod 664 $service_dir/subscribers.service
		sudo cp config_files/subscribers_fwd.service.u2004 $service_dir/subscribers_fwd.service
		sudo chmod 664 $service_dir/subscribers_fwd.service
		
		sudo systemctl enable subscribers.service
		sudo systemctl enable subscribers_fwd.service
	fi

	if [ $u1804 -eq 1 ]; then
		sudo cp config_files/subscribers.service $service_dir
		sudo chmod 664 $service_dir/subscribers.service
		sudo cp config_files/subscribers_fwd.service $service_dir
		sudo chmod 664 $service_dir/subscribers_fwd.service
		
		sudo systemctl enable subscribers.service
		sudo systemctl enable subscribers_fwd.service
	fi
	
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
        cat setupDb.sql | sudo -i -u postgres psql -d $sd | grep -v "does not exist, skipping"
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

echo '##### 14. Add imagemagick,ffmpeg to generate thumbnails'
if [ $(cat /etc/*-release | grep "DISTRIB_CODENAME=" | cut -d "=" -f2) = "trusty" ];
then  
sudo add-apt-repository ppa:mc3man/trusty-media  && sudo apt-get update -y
fi

sudo apt-get install imagemagick -y
sudo apt-get install ffmpeg -y 
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
if [ ! -f "/etc/default/$TOMCAT_VERSION.bu" ]; then
	sudo cp /etc/default/$TOMCAT_VERSION  /etc/default/$TOMCAT_VERSION.bu
fi
sudo sed -i "s#-Xmx128m#-Xmx512m#g" /etc/default/$TOMCAT_VERSION

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
sudo service $TOMCAT_VERSION start
sudo service apache2 start

echo '##### 20. Enable export to shape files, kmz files and pdf files'
sudo apt-get install zip -y
sudo apt-get install gdal-bin -y
sudo apt-get install ttf-dejavu -y

# Add a file containing the version number
echo "2405" > ~/smap_version

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

