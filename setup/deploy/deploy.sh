#!/bin/sh
deploy_from="version1"
# Where this server's components are, from /etc/smap/platform.conf, or detected and saved
# there on a server installed before it existed.  Then check the installed versions
. ../install/platform.sh
if ! platform_setup; then
    echo "Smap has not been deployed.  On a server still running Tomcat 9 run patchdb.sh first to migrate to Tomcat 10."
    exit 1
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
echo "SESSPASS=\"`cat /dev/urandom | tr -dc '[:alnum:]' | head -c 10`\"" >> /etc/environment

# Get location of database
# From /etc/environment if it is not in this environment, as on a new install, where install.sh has
# only just written it there and sudo does not pass it on.  Without it this would be taken for a
# remote database and the patches would not be applied
if [ -z "$DBHOST" ]; then
	DBHOST=`sed -n 's/^DBHOST=//p' /etc/environment | tr -d '"' | tail -1`
fi
DBHOST=${DBHOST:-127.0.0.1}; export DBHOST
if [ "$DBHOST" = "127.0.0.1" ]
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
systemctl stop $APACHE_SERVICE
systemctl stop $TOMCAT_SERVICE
systemctl stop subscribers
systemctl stop subscribers_fwd
if [ "$DBHOST" = "127.0.0.1" ]; then
    systemctl stop $PG_SERVICE
fi

cd $deploy_from
for f in `ls *.war`
do
    echo "restarting:" $f
    rm $TOMCAT_WEBAPPS/$f
    fdir=`echo $f | sed "s/\([a-zA-Z0-9]*\)\..*/\1/"`
    echo "deleting folder:" $fdir
    rm -rf $TOMCAT_WEBAPPS/$fdir
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
	tar -xzf $deploy_from/smapServer.tgz -C /var/www/smap --warning=no-unknown-keyword
	cp /var/www/smap/images/smap_logo.png /smap/misc
	chown -R $TOMCAT_USER /smap/misc
fi

if [ -e $deploy_from/fieldAnalysis.tgz ]
then
        echo "Updating fieldAnalysis"
        rm -rf /var/www/smap/fieldAnalysis
        rm -rf /var/www/smap/app/fieldAnalysis
        tar -xzf $deploy_from/fieldAnalysis.tgz -C /var/www/smap/app --warning=no-unknown-keyword
fi

if [ -e $deploy_from/fieldManager.tgz ]
then
        echo "Updating fieldManager"
        rm -rf /var/www/smap/fieldManager
        rm -rf /var/www/smap/app/fieldManager

        tar -xzf $deploy_from/fieldManager.tgz -C /var/www/smap/app --warning=no-unknown-keyword
fi

if [ -e $deploy_from/tasks.tgz ]
then
        echo "Updating tasks"
        rm -rf /var/www/smap/tasks
        rm -rf /var/www/smap/app/tasks
        tar -xzf $deploy_from/tasks.tgz -C /var/www/smap/app --warning=no-unknown-keyword
fi

if [ -e $deploy_from/myWork.tgz ]
then
        echo "Updating myWork"
        rm -rf /var/www/smap/myWork
        rm -rf /var/www/smap/app/myWork
        tar -xzf $deploy_from/myWork.tgz -C /var/www/smap/app --warning=no-unknown-keyword
fi

if [ -e $deploy_from/dashboard.tgz ]
then
        echo "Dashboard"
        rm -rf /var/www/smap/dashboard
        tar -xzf $deploy_from/dashboard.tgz -C /var/www/smap --warning=no-unknown-keyword
fi

cp $deploy_from/fieldTask.apk /var/www/smap
cp $deploy_from/meqa.apk /var/www/smap
cp $deploy_from/fieldTaskPreJellyBean.apk /var/www/smap
cp $deploy_from/smapFingerprint.apk /var/www/smap
cp $deploy_from/fpReader2.apk /var/www/smap
cp $deploy_from/smapUploader.jar /var/www/smap
cp $deploy_from/fieldTask.apk /var/www/default
#cp -r $deploy_from/smapIcons/WebContent/* /var/www/smap/smapIcons
cp $deploy_from/*.war $TOMCAT_WEBAPPS
chown -R $TOMCAT_USER $TOMCAT_WEBAPPS

# change owner for apache web directory
chown -R $APACHE_USER: /var/www/smap
chmod -R o-rwx /var/www/smap

#
# smap bin
cp ../install/subscribers.sh /smap_bin
cp $deploy_from/subscribers.jar /smap_bin
cp $deploy_from/codebook.jar /smap_bin
cp -r $deploy_from/subscribers/default /smap_bin
cp -r $deploy_from/resources /smap_bin
cp -r $deploy_from/scripts/* /smap_bin
mkdir -p /usr/share/fonts/truetype
cp  $deploy_from/resources/fonts/* /usr/share/fonts/truetype
chmod +x /smap_bin/*.sh
chmod +r /usr/share/fonts/truetype/*

#
# Libraries shared by the war files and by subscribers.jar, rather than one copy inside
# each of them.  The list is in shared-libs.txt in smapserver2; the war poms and the
# subscribers shade plugin exclude exactly these.  Replaced completely on each deploy so
# that old versions do not accumulate.
#
# Deployed here rather than under Tomcat because subscribers runs on servers built with
# WEBSITE=no, where there is no Tomcat at all.
#
if [ -d $deploy_from/lib ]
then
	SHARED_LIB=/smap_bin/lib
	rm -rf $SHARED_LIB
	mkdir -p $SHARED_LIB
	cp $deploy_from/lib/*.jar $SHARED_LIB
	chmod -R a+rX $SHARED_LIB
	echo "...... installed `ls $SHARED_LIB | wc -l | tr -d ' '` shared libraries in $SHARED_LIB"
fi

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
if [ "$DBHOST" = "127.0.0.1" ]; then
    systemctl start $PG_SERVICE
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
	# Ensure PostgreSQL JDBC driver is in Tomcat's shared lib before startup
	cp -r $deploy_from/jdbc/* $TOMCAT_LIB/

	#
	# Point Tomcat at the shared libraries installed in /smap_bin/lib above.  The shared
	# classloader is a parent of every web application, so one copy serves all of the war
	# files.  Tomcat only reads this at startup
	#
	if [ -d /smap_bin/lib ]
	then
		CATALINA_PROPS=$TOMCAT_CONF_DIR/catalina.properties
		if grep -q '^shared.loader="/smap_bin/lib/\*.jar"' $CATALINA_PROPS
		then
			echo "...... shared libraries already configured in $CATALINA_PROPS"
		else
			grep -q "^shared.loader=" $CATALINA_PROPS || echo "shared.loader=" >> $CATALINA_PROPS
			sed -i 's#^shared.loader=.*#shared.loader="/smap_bin/lib/*.jar"#' $CATALINA_PROPS
			echo "...... configured shared libraries in $CATALINA_PROPS"
		fi

		# Left behind by releases that kept the shared libraries under Tomcat
		rm -rf $TOMCAT_BASE_DIR/smaplib
	fi

	systemctl start $TOMCAT_SERVICE
	systemctl start $APACHE_SERVICE
fi

if [ "$SUBSCRIBER" != "no" ]
then
    echo "Cleaning subscriber queues"
    cat ./queueclean.sql | sudo -i -u postgres $PSQL -d survey_definitions 

    echo "...... starting subscriber"
    echo "go" > /smap/settings/subscriber
    systemctl enable subscribers
    systemctl start subscribers

    systemctl enable subscribers_fwd
    systemctl start subscribers_fwd
else
    # Disable subscribers so they will not restart on next reboot
    systemctl disable subscribers
    systemctl disable subscribers_fwd
fi
