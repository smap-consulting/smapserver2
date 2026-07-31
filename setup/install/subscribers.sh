#!/bin/sh
#
# The libraries listed in shared-libs.txt are not packaged inside subscribers.jar, they
# are deployed once to /smap_bin/lib and shared with the war files.  So the jar has to be
# started with -cp rather than -jar, which ignores the class path.
#
cd /smap_bin
. /smap_bin/setcredentials.sh
java $JAVAOPTIONS -cp "/smap_bin/subscribers.jar:/smap_bin/lib/*" Manager $1 $2 $3 >> /var/log/subscribers/subscriber_$1_$3.log 2>&1
