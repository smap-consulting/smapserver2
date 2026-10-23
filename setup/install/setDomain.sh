#!/bin/sh
##### Set the domain name that the Apache virtual hosts answer to
#
# Usage: sudo ./setDomain.sh example.com     Only accept requests for example.com
#        sudo ./setDomain.sh                 No domain, accept requests for any host name
#
# Called by install.sh, and can be run again later, for example when a server that was installed
# without a domain is given one.  It edits smap.conf and smap-ssl.conf where they are, so changes
# made since the install, such as the certificate lines certbot adds, are kept.
#
# The application builds links in emails from the host name a request arrives with, so with a
# domain set any request for another host name is refused, which stops a forged Host header
# putting another site in those links.  Without one that protection is not there.

domain=$1
a_config_dir=${A_CONFIG_DIR:-/etc/apache2/sites-available}

# Only the characters a host name can have, as the value is written into the Apache configuration
if [ -n "$domain" ] && ! echo "$domain" | grep -Eq '^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$'; then
	echo "Invalid domain name: $domain"
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
		/^<VirtualHost / && domain != "" {
			print "    ServerName " domain
			print "    <If \"req(" q "Host" q ") != " q domain q "\">"
			print "       Deny from all"
			print "    </If>"
		}
	' $f > $tmp && cat $tmp > $f
	rm -f $tmp
done

if [ -n "$domain" ]; then
	echo "Apache will only accept requests for $domain"
else
	echo "WARNING: No domain name set, Apache will accept requests for any host name."
	echo "Links in emails will use whatever host name a request arrives with."
	echo "When the server has a domain name run: sudo ./setDomain.sh <domain name>"
fi

if command -v systemctl > /dev/null && systemctl is-active --quiet apache2; then
	apache2ctl configtest && systemctl reload apache2
fi
