#!/bin/sh
#
# Run the pdf acceptance tests.  From the sdDAL directory:  test/pdf/run.sh
#
# They exercise the pdf code that does not need a database: image handling including webp,
# filling and flattening a template, the html label pipeline against default_pdf.css, and
# the resize and blank page utilities.  Nothing is written outside target.
#
# Java 11 runs the source file as it is, so there is nothing to compile, but sdDAL itself
# has to have been built.
#
set -e

cd `dirname $0`/../..					# sdDAL

if [ ! -d target/classes ]
then
	echo "Build sdDAL first: mvn compile"
	exit 1
fi

JAVA=${JAVA_HOME:+$JAVA_HOME/bin/}java
CP=target/pdf-test-classpath.txt

mvn -q dependency:build-classpath -Dmdep.outputFile=$CP

#
# The awt font warning and the reflective access warnings from iText 5 on Java 11 are
# noise, keep them out of the report but keep the exit status of the tests
#
out=`mktemp`
if $JAVA -cp "target/classes:`cat $CP`" test/pdf/PdfTests.java "$@" > $out 2>&1
then
	rc=0
else
	rc=$?
fi
grep -v "^WARNING\|^Warning: the fonts" $out || true
rm -f $out
exit $rc
