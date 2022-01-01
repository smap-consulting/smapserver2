[Smap Server](http://www.smap.com.au) 
======

The Smap Server manages survey definitions, stores submitted results and helps analyse those results.  Access to the server is via REST APIS which can be used to create your own data 
collection system.

Components
----------
* surveyMobileAPI. Web services used by data collection clients
* surveyKPI.  Web services used for administration
* koboToolboxAPI.  Data APIs based initially on the API use by kobo toolbox
* subscribers.  A batch program whose main job is to apply submitted XML files to the database.
* sdDAL.  A library of shared code.
* sdDataAccess.  A legacy library of shared code.
* setup.  Scripts.
* cloudInterface.  Dummy end points for access to cloud services.  (In particular AWS),
* codebook.  A fork of the odk codebook for generating codebooks from survey templates.

Follow the latest news about Smap on our [blog](http://blog.smap.com.au)

Development
-----------
* Code contributions are very welcome. 
* [Issue Tracker](https://github.com/smap-consulting/smapserver/issues)

*  Install Eclipse
*  Clone this project
*  Import the modules you want to work in from the file system
*  For each war file set the project facets as: Dynamic Web Module 3.0, Java 1.8, JAX-RS 1.1.

How to Install
--------------

The installation scripts currently are tested only on supported Ubuntu LTS versions and contain some lines specific to Ubuntu.

*  Copy the setup folder to the location on the linux server that you want to install the Smap server
*  Build the war files for surveyMobileAPI, surveyKPI, koboToolbox API and copy them the deploy/version1 folder under setup
*  Create a runnable jar file for subscribers and copy it to the deply/version1 folder
*  run the install script in the install folder as: sudo su install.sh

Upgrades
--------

*  Copy the updated war file or runnable jar file to the version1 folder
*  in the deploy folder run:
*    sudo ./patchdb.sh
*    sudo ./deploy.sh
