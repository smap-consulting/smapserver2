#!/bin/sh
##### Set the domain name, or IP address, that the Apache virtual hosts answer to
#
# Usage: sudo ./setDomain.sh smap.example.com
#        sudo ./setDomain.sh 203.0.113.10       A server without a domain name
#
# Called by install.sh, and can be run again later, for example when a server that was installed
# with an IP address is given a domain name.  It edits smap.conf and smap-ssl.conf where they are,
# so changes made since the install, such as the certificate lines certbot adds, are kept.
#
# The application builds links in emails from the host name a request arrives with, so any request
# for another host name is refused, which stops a forged Host header putting another site in those
# links.  There is no option to accept any host name, as that would remove this protection.

domain=$1

# Where this server's Apache is, saved by install.sh or deploy.sh
platform_conf=${SMAP_PLATFORM_CONF:-/etc/smap/platform.conf}
[ -f $platform_conf ] && . $platform_conf
a_config_dir=${A_CONFIG_DIR:-${APACHE_SITE_DIR:-/etc/apache2/sites-available}}
APACHE_SERVICE=${APACHE_SERVICE:-apache2}
APACHECTL=${APACHECTL:-apache2ctl}

# Only the characters a host name or IPv4 address can have, as the value is written into the Apache configuration
if ! echo "$domain" | grep -Eq '^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$'; then
	echo "Usage: sudo ./setDomain.sh {domain name or IP address}"
	[ -n "$domain" ] && echo "Invalid domain name or IP address: $domain"
	exit 1
fi

for f in $a_config_dir/smap.conf $a_config_dir/smap-ssl.conf
do
	if [ ! -f $f ]; then
		continue
	fi

	# Remove the current settings, the {hostname} placeholders on a new install, and put the new
	# ones at the start of the virtual host
	tmp=$(mktemp)
	awk -v domain="$domain" -v q="'" '
		/^[[:space:]]*<If "req\(.Host.\) != / { skip = 1; next }
		skip { if ($0 ~ /^[[:space:]]*<\/If>/) skip = 0; next }
		/^[[:space:]]*Server(Name|Alias)[[:space:]]/ { next }
		{ print }
		/^<VirtualHost / {
			print "    ServerName " domain
			print "    <If \"req(" q "Host" q ") != " q domain q "\">"
			print "       Require all denied"
			print "    </If>"
		}
	' $f > $tmp && cat $tmp > $f
	rm -f $tmp
done

echo "Apache will only accept requests for $domain"

if command -v systemctl > /dev/null && systemctl is-active --quiet $APACHE_SERVICE; then
	$APACHECTL configtest && systemctl reload $APACHE_SERVICE
fi
