#!/bin/sh
deploy_from="version1"

# Get location of database
if [ $DBHOST = "127.0.0.1" ]
then
        echo "local database"
        PSQL="psql"
else
        echo "remote database"
        PSQL="PGPASSWORD=keycar08 psql -h $DBHOST -U postgres"
fi

# Set flag for ubuntu version
u1404=`lsb_release -r | grep -c "14\.04"`
u1604=`lsb_release -r | grep -c "16\.04"`
u1804=`lsb_release -r | grep -c "18\.04"`
u2004=`lsb_release -r | grep -c "20\.04"`
u2204=`lsb_release -r | grep -c "22\.04"`
u2404=`lsb_release -r | grep -c "24\.04"`

if [ $u2404 -eq 1 ]; then
    TOMCAT_VERSION=tomcat9
    TOMCAT_USER=tomcat
elif [ $u2204 -eq 1 ]; then
    TOMCAT_VERSION=tomcat9
    TOMCAT_USER=tomcat
elif [ $u2004 -eq 1 ]; then
    TOMCAT_VERSION=tomcat9
    TOMCAT_USER=tomcat
elif [ $u1804 -eq 1 ]; then
    TOMCAT_VERSION=tomcat8
else
    TOMCAT_VERSION=tomcat7
fi

CATALINA_HOME=/usr/share/$TOMCAT_VERSION

# Copy postgres driver
cp -r $deploy_from/jdbc/* $CATALINA_HOME/lib/ 

echo '##### 0. check configuration'
# Set flag if this is apache2.4
a24=`sudo apachectl -version | grep -c "2\.4"`
a_config_dir="/etc/apache2/sites-available"
if [ $a24 -eq 0 ]; then	
	echo "%%%%%% Warning: Apache configuration files for Apache 2.4 will be installed. "
	echo "PLease install Apache 2.4 prior to upgrading"
	exit 1;
fi

version=2504
if [ -e /smap_bin/smap_version ]; then
        version=`sudo cat /smap_bin/smap_version`
elif [ -e ~/smap_version ]; then
        version=`sudo cat ~/smap_version`
        cp ~/smap_version /smap_bin
fi

echo "Current Smap Version is $version"

# Apply database patches

if [ $version -lt "1908" ]
then
echo "applying pre 1909 patches to survey_definitions"
cat ./sd_pre_1908.sql | sudo -i -u postgres $PSQL -q -d survey_definitions 2>&1 | grep -v "already exists" | grep -v "duplicate key" | grep -vi "addgeometrycolumn" | grep -v "implicit index" | grep -v "skipping" | grep -v "is duplicated" | grep -v "create unique index" | grep -v CONTEXT
fi

if [ $version -lt "2306" ]
then
echo "applying pre 2306 patches to survey_definitions"
cat ./sd_pre_2306.sql | sudo -i -u postgres $PSQL -q -d survey_definitions 2>&1 | grep -v "already exists" | grep -v "duplicate key" | grep -vi "addgeometrycolumn" | grep -v "implicit index" | grep -v "skipping" | grep -v "is duplicated" | grep -v "create unique index" | grep -v CONTEXT
fi

echo "applying new patches to survey_definitions"
cat ./sd.sql | sudo -i -u postgres $PSQL -q -d survey_definitions 2>&1 | grep -v "already exists" | grep -v "duplicate key" | grep -vi "addgeometrycolumn" | grep -v "implicit index" | grep -v "skipping" | grep -v "is duplicated" | grep -v "create unique index" | grep -v CONTEXT | grep -v "does not exist"

echo "applying patches to results"
cat ./results.sql | sudo -i -u postgres $PSQL -q -d results 2>&1 | grep -v "already exists"

echo "Archiving records in survey_definitions"
cat ./archive.sql | sudo -i -u postgres $PSQL -q -d survey_definitions 2>&1 

# Version 14.02
if [ $version -lt "1402" ]
then
	echo "Applying patches for version 14.02"
	sudo apt-get install gdal-bin
fi

# version 14.03
if [ $version -lt "1403" ]
then
	echo "Applying patches for version 14.03"
	echo "set up forwarding - assumes uploaded files are under /smap"
        sudo cp -v  ../install/config_files/subscribers_fwd.conf /etc/init
        sudo cp -v  ../install/subscribers.sh /smap_bin
	sudo sed -i "s#{your_files}#/smap#g" /etc/init/subscribers_fwd.conf
	echo "Modifying URLs of attachments to remove hostname, also moving uploaded files to facilitate forwarding of old surveys"
	java -jar version1/patch.jar apply survey_definitions results
	sudo chown -R $TOMCAT_version /smap/uploadedSurveys
fi

# version 14.08
if [ $version -lt "1408" ]
then
echo "Applying patches for version 14.08"
echo "installing ffmpeg"
if [ $(cat /etc/*-release | grep "DISTRIB_CODENAME=" | cut -d "=" -f2) == 'trusty' ];
then  
sudo add-apt-repository 'deb  http://ppa.launchpad.net/jon-severinsson/ffmpeg/ubuntu trusty main'  && sudo add-apt-repository 'deb  http://ppa.launchpad.net/jon-severinsson/ffmpeg/ubuntu saucy main'  && sudo apt-get update
fi
sudo apt-get update -y
sudo apt-get install ffmpeg -y
fi

# version 14.10

if [ $version -lt "1410" ]
then
echo "Applying patches for version 14.10"
sudo chown -R $TOMCAT_version /var/log/subscribers
fi

# version 14.11
# Yes 14.11 patches being reapplied as last release was actually 14.10.02
if [ $version -lt "1411" ]
then
echo "Applying patches for version 14.11"
sudo chown -R $TOMCAT_version /smap/attachments
fi


# version 15.01
if [ $version -lt "1501" ]
then
echo "Applying patches for version 15.01"
sudo mkdir /smap/media/organisation
sudo chown -R $TOMCAT_version /smap/media
fi

# version 15.02
if [ $version -lt "1502" ]
then
echo "Applying patches for version 15.02"
sudo rm /var/lib/$TOMCAT_VERSION/webapps/fieldManager.war
fi

# version 15.03
if [ $version -lt "1503" ]
then
sudo mkdir /smap_bin/resources
sudo mkdir /smap_bin/resources/css
fi

# version 15.09
if [ $version -lt "1509" ]
then

# Patch the database
java -jar version1/patch1505.jar apply survey_definitions results

cd ../install
# Set up new apache configuration structure

if [ ! -f "$a_config_dir/smap.conf.bu" ]; then
    sudo cp  $a_config_dir/smap.conf $a_config_dir/smap.conf.bu
fi
sudo cp config_files/a24-smap.conf $a_config_dir/smap.conf

if [ ! -f "$a_config_dir/smap-ssl.conf.bu" ]; then
    sudo cp $a_config_dir/smap-ssl.conf $a_config_dir/smap-ssl.conf.bu
fi
sudo cp config_files/a24-smap-ssl.conf $a_config_dir/smap-ssl.conf

sudo a2ensite  smap.conf
sudo a2ensite  smap-ssl.conf

# Disable default sites - TODO find some way of smap coexistign with other sites on the same apache server automatically
sudo a2dissite 000-default
sudo a2dissite default-ssl

cd ../deploy

# Create miscelaneous directory
sudo mkdir /smap/misc
sudo chown $TOMCAT_version /smap/misc

fi

# version 15.11
if [ $version -lt "1511" ]
then
java -jar version1/patch.jar apply survey_definitions results
fi

# version 16.01
if [ $version -lt "1601" ]
then
	java -jar version1/patchcomplete.jar apply survey_definitions results
fi

# version 16.02
if [ $version -lt "1602" ]
then
	echo "no patches for 16.02"
fi

# version 16.03
if [ $version -lt "1603" ]
then
	echo "no patches for 16.03"
fi

# version 16.12
if [ $version -lt "1612" ]
then
echo "Applying patches for version 16.12"
sudo mkdir /smap_bin
sudo mkdir /smap_bin/resources
sudo mkdir /smap_bin/resources/css

sudo apt-get install python-dev -y
sudo apt-get install libxml2-dev -y
sudo apt-get install libxslt-dev
sudo apt-get install libxslt1-dev -y
sudo apt-get install git -y
sudo apt-get install python-setuptools -y
sudo easy_install pip
sudo pip install setuptools --no-use-wheel --upgrade
sudo pip install xlrd
sudo rm -rf src/pyxform
sudo pip install -e git+https://github.com/UW-ICTD/pyxform.git@master#egg=pyxform
sudo rm -rf /smap_bin/pyxform
sudo cp -r src/pyxform/pyxform/ /smap_bin
sudo a2enmod headers

sudo chown -R $TOMCAT_version /smap_bin

fi

# version 16.12
if [ $version -lt "1612" ]
then
	echo '# copy subscriber upstart files'
	upstart_dir="/etc/init"			
	service_dir="/etc/systemd/system"
	if [ $u1910 -eq 1 ]; then
		sudo cp ../install/config_files/subscribers.service $service_dir
		sudo chmod 664 $service_dir/subscribers.service
		sudo cp ../install/config_files/subscribers_fwd.service $service_dir
		sudo chmod 664 $service_dir/subscribers_fwd.service
		
		sudo sed -i "s#tomcat7#tomcat8#g" $service_dir/subscribers.service
		sudo sed -i "s#tomcat7#tomcat8#g" $service_dir/subscribers_fwd.service
	fi
	
	if [ $u1804 -eq 1 ]; then
		sudo cp ../install/config_files/subscribers.service $service_dir
		sudo chmod 664 $service_dir/subscribers.service
		sudo cp ../install/config_files/subscribers_fwd.service $service_dir
		sudo chmod 664 $service_dir/subscribers_fwd.service
		
		sudo sed -i "s#tomcat7#tomcat8#g" $service_dir/subscribers.service
		sudo sed -i "s#tomcat7#tomcat8#g" $service_dir/subscribers_fwd.service
	fi
	
	if [ $u1604 -eq 1 ]; then
		sudo cp ../install/config_files/subscribers.service $service_dir
		sudo chmod 664 $service_dir/subscribers.service
		sudo cp ../install/config_files/subscribers_fwd.service $service_dir
		sudo chmod 664 $service_dir/subscribers_fwd.service
	fi
	
	if [ $u1404 -eq 1 ]; then
		sudo cp ../install/config_files/subscribers.conf $upstart_dir
		sudo cp ../install/config_files/subscribers_fwd.conf $upstart_dir
	fi

fi

# Version 24.04
if [ $version -lt "2410" ]; then
	sudo a2enmod session
	sudo a2enmod request
	sudo a2enmod auth_form
	sudo a2enmod session_cookie
	sudo a2enmod session_crypto
fi

if [ ! -d "/smap/settings" ]
then
    mkdir /smap/settings
    cp ~ubuntu/region /smap/settings
    cp ~ubuntu/hostname /smap/settings
    cp ~ubuntu/bucket /smap/settings
    cp ~ubuntu/subscriber /smap/settings
fi

#####################################################################################
# All versions
# Copy the new apache configuration files and tomcat directory access
# Copy aws credentials

if [ $u2404 -eq 1 ]; then
    sudo cp  $deploy_from/resources/properties/credentials /var/lib/$TOMCAT_VERSION/.aws
elif [ $u2204 -eq 1 ]; then
    sudo cp  $deploy_from/resources/properties/credentials /var/lib/$TOMCAT_VERSION/.aws
elif [ $u2004 -eq 1 ]; then
    sudo cp  $deploy_from/resources/properties/credentials /var/lib/$TOMCAT_VERSION/.aws
elif [ $u1804 -eq 1 ]; then
    sudo cp  $deploy_from/resources/properties/credentials /var/lib/$TOMCAT_VERSION/.aws
else
    sudo cp  $deploy_from/resources/properties/credentials /usr/share/$TOMCAT_VERSION/.aws
fi
# update existing credentials
if [ -f $deploy_from/resources/properties/credentials ]
then
    for f in `locate .aws/credentials`
    do
            echo "processing $f"
            cp $deploy_from/resources/properties/credentials $f
    done
fi
sudo cp  $deploy_from/resources/properties/setcredentials.sh /smap_bin
envset=`cat /usr/share/$TOMCAT_VERSION/bin/setenv.sh | grep -c "setcredentials"`
if [ $envset -eq 0 ]; then
	echo ". /smap_bin/setcredentials.sh" | sudo tee -a /usr/share/$TOMCAT_VERSION/bin/setenv.sh 
fi


cd ../install
chmod +x apacheConfig.sh
./apacheConfig.sh

if [ $u2404 -eq 1 ]; then
cp config_files/override.conf /etc/systemd/system/tomcat9.service.d/override.conf
fi
if [ $u2204 -eq 1 ]; then
cp config_files/override.conf /etc/systemd/system/tomcat9.service.d/override.conf
fi
if [ $u2004 -eq 1 ]; then
cp config_files/override.conf /etc/systemd/system/tomcat9.service.d/override.conf
fi

cd ../deploy
 

# Get the AWS language codes
cp language_codes.csv /smap_bin
echo "truncate language_codes" | sudo -i -u postgres $PSQL -d survey_definitions
echo "\COPY language_codes (code, aws_translate, aws_transcribe, transcribe_default, transcribe_medical) FROM '/smap_bin/language_codes.csv' DELIMITER ',' CSV HEADER;" | sudo -i -u postgres $PSQL -d survey_definitions

# Set the full version in the database for the about page
echo "Setting version for about page"
full_version=`cat ./full_version`
echo "update server set version = '$full_version'" | sudo -i -u postgres $PSQL -q -d survey_definitions 2>&1

# update version reference
echo "2504" > /smap_bin/smap_version
