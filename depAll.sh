#!/bin/sh
cd smapServer
./dep.sh $1
cd ..

cd fieldAnalysis
./dep.sh
cd ..

cd tasks
./dep.sh
cd ..

cd fieldManagerClient
./dep.sh
cd ..
