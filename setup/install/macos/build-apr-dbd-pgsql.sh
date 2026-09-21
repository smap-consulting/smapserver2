#!/bin/sh
#
# Build apr_dbd_pgsql and install it into the Homebrew apr-util keg.
#
# mod_dbd reaches Postgres through an apr-util DBD driver, which apr-util loads as a
# separate shared object at run time.  Homebrew's apr-util formula configures with
# --without-pgsql, so the keg ships apr_dbd_sqlite3 and nothing else and Apache reports
#
#     Syntax error ... : No driver for pgsql
#
# There is no bottle or formula option to turn this on, so the one driver has to be built
# by hand from the same apr-util release Homebrew installed and dropped in beside the
# sqlite3 one.  Nothing else from this build is used.
#
# Apple's own Apache cannot be made to do this at all: /usr/sbin/httpd links the apr-util
# in the dyld shared cache, which is built with DSO driver loading disabled and only
# sqlite3 compiled in, so it has no directory to drop a driver into.  That is why the
# localhost setup uses the Homebrew Apache - see README.md in this directory.
#
# Re-run this after every "brew upgrade apr-util": the upgrade replaces the keg and takes
# the driver with it.
#
set -e

BREW=`brew --prefix`
APU="$BREW/opt/apr-util"
DRIVER_DIR="$APU/lib/apr-util-1"

if [ ! -d "$DRIVER_DIR" ]; then
    echo "ERROR: $DRIVER_DIR not found - is apr-util installed?  brew install apr-util"
    exit 1
fi
if [ ! -d "$BREW/opt/libpq" ]; then
    echo "ERROR: libpq not found - brew install libpq"
    exit 1
fi

# Build the same version that is installed.  A driver is only loadable by the apr-util it
# was built against, so taking the version from the keg rather than hard coding one means
# this still does the right thing after an upgrade.
VERSION=`"$APU/bin/apu-1-config" --version`
TARBALL="apr-util-$VERSION.tar.bz2"
WORK=`mktemp -d /tmp/apr-dbd-pgsql.XXXXXX`
trap 'rm -rf "$WORK"' EXIT

echo "Building apr_dbd_pgsql for apr-util $VERSION"

cd "$WORK"
curl -fsSL -O "https://archive.apache.org/dist/apr/$TARBALL"

# Check against the checksum Homebrew used for the release it installed, so this is the
# same source and not whatever the mirror happens to be serving.
EXPECTED=`brew info --json=v2 apr-util 2>/dev/null \
    | /usr/bin/python3 -c 'import json,sys; print(json.load(sys.stdin)["formulae"][0]["urls"]["stable"]["checksum"])'`
ACTUAL=`shasum -a 256 "$TARBALL" | cut -d" " -f1`
if [ -n "$EXPECTED" ] && [ "$EXPECTED" != "$ACTUAL" ]; then
    echo "ERROR: checksum mismatch for $TARBALL"
    echo "  expected $EXPECTED"
    echo "  got      $ACTUAL"
    exit 1
fi

tar xjf "$TARBALL"
cd "apr-util-$VERSION"

# Same arguments as the Homebrew formula, plus --with-pgsql.  Only dbd/apr_dbd_pgsql is
# taken from the result, but apr-util has no target that builds a driver on its own.
./configure --prefix="$WORK/out" \
    --with-apr="$BREW/opt/apr" \
    --with-crypto --with-openssl="$BREW/opt/openssl@3" \
    --with-pgsql="$BREW/opt/libpq" > configure.log 2>&1
make > make.log 2>&1

SO="apr_dbd_pgsql-1.so"
cp "dbd/.libs/$SO" "$DRIVER_DIR/$SO"
# libtool leaves the build directory in the install name; a bundle opened by dlopen does
# not need one, but a stale path in there is misleading to anyone running otool on it.
install_name_tool -id "$DRIVER_DIR/$SO" "$DRIVER_DIR/$SO"
chmod 444 "$DRIVER_DIR/$SO"
ln -sf "$SO" "$DRIVER_DIR/apr_dbd_pgsql.so"

echo "Installed $DRIVER_DIR/$SO"
otool -L "$DRIVER_DIR/$SO"
