# Smap server on a Mac, for development

Apache on this machine authenticates the console, the API and the mobile clients against
the `users` table in `survey_definitions`, through `mod_dbd`, exactly as a deployed server
does.  That means the one set of rules in
`setup/install/config_files/a24-smap-volatile.conf` is what runs here too, so a change to
access control is tested where it will be used rather than against a local password file
that has to be kept in step by hand.

## Why not the Apache in /usr/sbin

It cannot do this, and no amount of configuration will make it.  `/usr/sbin/httpd` links
the `apr-util` inside the dyld shared cache, which Apple builds with DSO driver loading
turned off and only the sqlite3 DBD driver compiled in.  `mod_dbd.so` and
`mod_authn_dbd.so` are present, so the failure looks like a configuration problem, but:

    DBDriver sqlite3   ->  Syntax OK
    DBDriver pgsql     ->  Syntax error ... : No driver for pgsql

`No driver for pgsql` is `APR_ENOTIMPL`: not "I could not load the driver" but "I have no
way to load one".  There is no directory to install a driver into and the library cannot
be replaced.

The Homebrew Apache uses the Homebrew `apr-util`, which does load drivers from
`$(brew --prefix)/opt/apr-util/lib/apr-util-1`.  Its formula configures with
`--without-pgsql`, so the driver still has to be built, which is what
`build-apr-dbd-pgsql.sh` does.

## Setup

    brew install httpd libpq
    setup/install/macos/build-apr-dbd-pgsql.sh
    setup/install/macos/install.sh
    sudo apachectl stop                 # Apple's httpd, if it is still running
    sudo brew services start httpd

`install.sh` links `/etc/apache2/other/smap-volatile` to the copy of
`a24-smap-volatile.conf` in this working copy, and copies `httpd.conf` from this directory
to `$(brew --prefix)/etc/httpd/httpd.conf`, rewriting the Homebrew prefix on the way so
that it works on an Intel Mac as well.  Anything it replaces is kept alongside with a
`.bu` suffix.

Because the rules are a symlink there is nothing to deploy on a build: a pull or an edit
is picked up by `sudo brew services restart httpd`.

`sudo` is needed to start Apache because this listens on port 80, not on Homebrew's
default of 8080, so that the console and the mobile clients can keep using bare host
names.

## After a brew upgrade

`brew upgrade apr-util` replaces the keg and takes the Postgres driver with it, and Apache
then refuses to start with `No driver for pgsql`.  Re-run:

    setup/install/macos/build-apr-dbd-pgsql.sh

It builds whichever apr-util version is now installed, so it needs no maintenance of its
own.  `brew upgrade httpd` leaves `httpd.conf` alone.

## Local settings

`httpd.conf` here is committed, so anything specific to one machine - in particular the
session encryption passphrase, which a deployed server takes from `/etc/environment` -
goes in `$(brew --prefix)/etc/httpd/smap-local.conf`, which is read if it exists:

    Define SESSPASS <passphrase>

## Checking it

A wrong password for a user who exists and one for a user who does not give different
messages in `$(brew --prefix)/var/log/httpd/error_log`, which is the quickest way to see
that the query is really reaching Postgres:

    user kim: authentication failure for "/api/v1/data": Password Mismatch
    user nosuchuser99 not found: /api/v1/data

Only the accounts with a `basic_password` can use the console; the rest still have only
the MD5 digest that the device API checks:

    select count(*) from users where basic_password is null;
