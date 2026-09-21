#!/bin/sh
#
# Point the Homebrew Apache at this working copy, for a Smap server running on a Mac for
# development.  Run once - after that a git pull is enough, because the Apache rules are
# a symlink into the repository rather than a copy of it.
#
#   brew install httpd libpq
#   setup/install/macos/build-apr-dbd-pgsql.sh
#   setup/install/macos/install.sh
#   sudo apachectl stop                 # Apple's httpd, if it is still running
#   sudo brew services start httpd      # sudo because this listens on port 80
#
# Needs sudo for the symlink into /etc.
#
set -e

DIR=$(cd "$(dirname "$0")" && pwd)
BREW=$(brew --prefix)
VOLATILE="$DIR/../config_files/a24-smap-volatile.conf"
TARGET=/etc/apache2/other/smap-volatile
HTTPD_CONF="$BREW/etc/httpd/httpd.conf"

if [ ! -f "$BREW/opt/apr-util/lib/apr-util-1/apr_dbd_pgsql-1.so" ]; then
    echo "The Postgres DBD driver is not installed, so mod_dbd cannot authenticate.  Run:"
    echo "  $DIR/build-apr-dbd-pgsql.sh"
    exit 1
fi

# A symlink, not a copy, so the file Apache reads is the file in the repository and the
# two cannot drift.  Left at the path the old file based setup used, so nothing else has
# to be told about the move.
echo "Linking $TARGET"
if [ -e "$TARGET" ] && [ ! -L "$TARGET" ]; then
    # Dated, because there is already a smap-volatile.bu on a machine that was set up by
    # hand and it is the only copy of whatever was working before.
    BU="$TARGET.bu.$(date +%Y%m%d%H%M%S)"
    sudo mv "$TARGET" "$BU"
    echo "  the file that was there is now $BU"
fi
sudo ln -sfn "$(cd "$(dirname "$VOLATILE")" && pwd)/$(basename "$VOLATILE")" "$TARGET"

echo "Installing $HTTPD_CONF"
if [ -f "$HTTPD_CONF" ] && [ ! -f "$HTTPD_CONF.bu" ]; then
    cp "$HTTPD_CONF" "$HTTPD_CONF.bu"
    echo "  the file that was there is now $HTTPD_CONF.bu"
fi
# httpd.conf is written with the Apple Silicon Homebrew prefix, so that it can be read
# and syntax checked as it stands.  Rewrite it to wherever brew actually is, which is
# /usr/local on an Intel Mac.  A no-op on /opt/homebrew.
sed "s|/opt/homebrew|$BREW|g" "$DIR/httpd.conf" > "$HTTPD_CONF"

"$BREW/opt/httpd/bin/httpd" -t -f "$HTTPD_CONF"
echo "Done.  Restart Apache with: sudo brew services restart httpd"
