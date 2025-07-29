#!/bin/sh
deploy_from="version1"
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
    TOMCAT_USER=tomcat8
else
    TOMCAT_VERSION=tomcat7
    TOMCAT_USER=tomcat7
fi

# Add a default setting of "yes" as the value for SUBSCRIBER if it has not already been set
subscribers_set=`grep SUBSCRIBER /etc/environment | wc -l`
if [ $subscribers_set -eq 0 ]; then
    echo "SUBSCRIBER=yes" >> /etc/environment
    SUBSCRIBER=yes; export SUBSCRIBER;
fi

# Add a default setting of "yes" as the value for WEBSITE if it has not already been set
website_set=`grep WEBSITE /etc/environment | wc -l`
if [ $website_set -eq 0 ]; then
    echo "WEBSITE=yes" >> /etc/environment
    WEBSITE=yes; export WEBSITE;
fi

# Update session password - generate a random 10 character alphanumeric string
sed '/SESSPASS/d' /etc/environment > /etc/environment.temp
mv /etc/environment.temp /etc/environment
echo "export SESSPASS=\"`cat /dev/urandom | tr -dc '[:alnum:]' | head -c 10`\"" >> /etc/environment

# Get location of database
if [ $DBHOST = "127.0.0.1" ]
then
        echo "local database"
        PSQL="psql"
else
        echo "remote database"
        PSQL="PGPASSWORD=keycar08 psql -h $DBHOST -U postgres"
fi

# save directory that contains deploy script
cwd=`pwd`

#
# stop services
#
service apache2 stop
service $TOMCAT_VERSION stop
if [ $u1404 -eq 1 ]; then
    service subscribers stop
    service subscribers_fwd stop
else
    systemctl stop subscribers
    systemctl stop subscribers_fwd
fi
if [ $DBHOST = "127.0.0.1" ]; then
    service postgresql stop
fi

cd $deploy_from
for f in `ls *.war`
do
    echo "restarting:" $f
    rm /var/lib/$TOMCAT_VERSION/webapps/$f
    fdir=`echo $f | sed "s/\([a-zA-Z0-9]*\)\..*/\1/"`
    echo "deleting folder:" $fdir
    rm -rf /var/lib/$TOMCAT_VERSION/webapps/$fdir
done
cd ..

if [ -e $deploy_from/smapServer.tgz ]
then
	echo "Updating smapServer"
	rm -rf /var/www/smap/OpenLayers
	rm -rf /var/www/smap/js
	rm -rf /var/www/smap/css
	rm -rf /var/www/smap/*.html
	rm -rf /var/www/smap/*.js
        rm -rf /var/www/smap/*.json
	tar -xzf $deploy_from/smapServer.tgz -C /var/www/smap
	cp /var/www/smap/images/smap_logo.png /smap/misc
	chown -R $TOMCAT_USER /smap/misc
fi

if [ -e $deploy_from/fieldAnalysis.tgz ]
then
        echo "Updating fieldAnalysis"
        rm -rf /var/www/smap/fieldAnalysis
        rm -rf /var/www/smap/app/fieldAnalysis
        tar -xzf $deploy_from/fieldAnalysis.tgz -C /var/www/smap/app
fi

if [ -e $deploy_from/fieldManager.tgz ]
then
        echo "Updating fieldManager"
        rm -rf /var/www/smap/fieldManager
        rm -rf /var/www/smap/app/fieldManager

        tar -xzf $deploy_from/fieldManager.tgz -C /var/www/smap/app
fi

if [ -e $deploy_from/tasks.tgz ]
then
        echo "Updating tasks"
        rm -rf /var/www/smap/tasks
        rm -rf /var/www/smap/app/tasks
        tar -xzf $deploy_from/tasks.tgz -C /var/www/smap/app
fi

if [ -e $deploy_from/myWork.tgz ]
then
        echo "Updating myWork"
        rm -rf /var/www/smap/myWork
        rm -rf /var/www/smap/app/myWork
        tar -xzf $deploy_from/myWork.tgz -C /var/www/smap/app
fi

if [ -e $deploy_from/dashboard.tgz ]
then
        echo "Dashboard"
        rm -rf /var/www/smap/dashboard
        tar -xzf $deploy_from/dashboard.tgz -C /var/www/smap
fi

cp $deploy_from/fieldTask.apk /var/www/smap
cp $deploy_from/meqa.apk /var/www/smap
cp $deploy_from/fieldTaskPreJellyBean.apk /var/www/smap
cp $deploy_from/smapFingerprint.apk /var/www/smap
cp $deploy_from/fpReader2.apk /var/www/smap
cp $deploy_from/smapUploader.jar /var/www/smap
cp $deploy_from/fieldTask.apk /var/www/default
#cp -r $deploy_from/smapIcons/WebContent/* /var/www/smap/smapIcons
cp $deploy_from/*.war /var/lib/$TOMCAT_VERSION/webapps
chown -R $TOMCAT_USER /var/lib/$TOMCAT_VERSION/webapps

# change owner for apache web directory
chown -R www-data:www-data /var/www/smap
chmod -R o-rwx /var/www/smap

#
# smap bin
cp ../install/subscribers.sh /smap_bin
cp $deploy_from/subscribers.jar /smap_bin
cp $deploy_from/codebook.jar /smap_bin
cp -r $deploy_from/subscribers/default /smap_bin
cp -r $deploy_from/resources /smap_bin
cp -r $deploy_from/scripts/* /smap_bin
cp  $deploy_from/resources/fonts/* /usr/share/fonts/truetype
chmod +x /smap_bin/*.sh
chmod +r /usr/share/fonts/truetype/*

cd /var/log/subscribers
rm *.log_old
#rename 's/\.log$/\.log_old/g' *.log  # Remove this now will rely on log rotation
cd $cwd

# Copy any customised files
if [ -e ../../custom/web ]
then
        echo "copy custom web files"
        cp -vr ../../custom/web/* /var/www/smap
fi
if [ -e ../../custom/misc ]
then
        echo "copy custom subscriber data files"
        cp -vr ../../custom/misc/* /smap_bin/misc
fi

# Restart Servers
if [ $DBHOST = "127.0.0.1" ]; then
    service postgresql start
    echo "Starting postgres"

else
    echo ".............. using remote postgres at $DBHOST"
    # Update configuration files that access the database
    sudo sed -i "s#127.0.0.1#$DBHOST#g" /smap_bin/default/metaDataModel.xml
    sudo sed -i "s#127.0.0.1#$DBHOST#g" /smap_bin/default/results_db.xml
    sudo sed -i "s#127.0.0.1#$DBHOST#g" /smap_bin/getshape.sh
fi

if [ "$WEBSITE" != "no" ]
then
	service $TOMCAT_VERSION start
	service apache2 start
fi

if [ "$SUBSCRIBER" != "no" ]
then
    echo "Cleaning subscriber queues"
    cat ./queueclean.sql | sudo -i -u postgres $PSQL -d survey_definitions 

    echo "...... starting subscriber"
    echo "go" > /smap/settings/subscriber
    if [ $u1404 -eq 1 ]; then
        service subscribers start
        service subscribers_fwd start
    else	
        systemctl enable subscribers
        systemctl start subscribers

        systemctl enable subscribers_fwd
        systemctl start subscribers_fwd
    fi
else
    # Disable subscribers so they will not restart on next reboot
    systemctl disable subscribers
    systemctl disable subscribers_fwd
fi
