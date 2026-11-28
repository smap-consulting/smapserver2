#!/bin/sh
#
# Run the XForm upload tests.  From the sdDAL directory:  test/xform/run.sh
#
# They load XForms into the survey model without a database: field classification for an
# XForm replace, pyxform and Smap style XForms, problem XForms, keeping settings on replace,
# and the XLSForm checks that are shared with the XForm upload.
#
set -e

cd `dirname $0`/../..					# sdDAL

if [ ! -d target/classes ]
then
	echo "Build sdDAL first: mvn compile"
	exit 1
fi

JAVA=${JAVA_HOME:+$JAVA_HOME/bin/}java
CP=target/xform-test-classpath.txt

mvn -q dependency:build-classpath -Dmdep.outputFile=$CP

out=`mktemp`
if $JAVA -cp "target/classes:`cat $CP`" test/xform/XFormTests.java "$@" > $out 2>&1
then
	rc=0
else
	rc=$?
fi
cat $out
rm -f $out
exit $rc
