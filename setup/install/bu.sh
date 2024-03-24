#!/bin/sh
export PATH=/usr/local/bin:$PATH
export HOME=/var/lib/postgresql

# back up the databases on this host
bucket=`cat /smap/settings/bucket`
file="smap_bu.tgz"
final_file="`cat /smap/settings/hostname`-`date +\%Y-\%m-\%d`-smap_bu.tgz.gpg"
rm -rf backups/*
rm $file
rm $file.gpg

# Dump databases
pg_dump -c -Fc survey_definitions > backups/sd.dmp
pg_dump -c -Fc results > backups/results.dmp

tar -zcf $file backups/*
rm -rf backups/*

# Encrypt
echo `cat passwordfile` | gpg --batch -q --passphrase-fd 0 --cipher-algo AES256 -c $file
rm $file

# Copy encrypted file to s3
echo "copy to s3"
aws s3 cp $file.gpg s3://{db-bu-bucket}/$final_file
rm $file.gpg

# Synchronise other files
aws s3 sync /smap s3://$bucket --exclude "temp/*"  --exclude "reports/*" --exclude "settings/*"
