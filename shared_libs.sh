#!/bin/sh
#
# Build the directory of libraries that are deployed to the Tomcat shared classloader
# rather than being packaged inside each war file.  deploy.sh installs it.
#
# The libraries to share are listed in shared-libs.txt.  The war poms exclude exactly
# these file names, so a library that is missing here would be missing at run time - the
# build stops if any of them cannot be found.
#
set -e

LIST=shared-libs.txt
OUT=~/deploy/smap/deploy/version1/lib
STAGE=surveyKPI/target/all-dependencies

#
# Resolve every runtime dependency of surveyKPI, it has the largest dependency set
#
cd surveyKPI
mvn -q dependency:copy-dependencies -DincludeScope=runtime \
	-DoutputDirectory=target/all-dependencies
cd ..

rm -rf $OUT
mkdir -p $OUT

missing=0
while read -r jar
do
	case "$jar" in
		''|\#*) continue ;;
	esac
	if [ -f "$STAGE/$jar" ]
	then
		cp "$STAGE/$jar" $OUT
	else
		echo "ERROR: shared library not found: $jar"
		missing=`expr $missing + 1`
	fi
done < $LIST

if [ $missing -gt 0 ]
then
	echo "$missing shared libraries were not found. Update $LIST and the war poms to match"
	exit 1
fi

#
# A library that is both shared and packaged in a war wastes space and risks two copies
# of the same classes, so report any that the poms have not excluded
#
for war in surveyMobileAPI/target/*.war koboToolboxApi/target/*.war surveyKPI/target/*.war
do
	[ -f "$war" ] || continue
	for jar in `unzip -Z1 "$war" 'WEB-INF/lib/*' | sed 's#.*/##'`
	do
		if [ -f "$OUT/$jar" ]
		then
			echo "WARNING: `basename $war` also contains shared library $jar"
		fi
	done
done

echo "Shared libraries: `ls $OUT | wc -l | tr -d ' '` jars, `du -sh $OUT | cut -f1`"
