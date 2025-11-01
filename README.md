[Smap Server](http://www.smap.com.au) 
======

The Smap Server manages survey definitions, stores submitted results and helps analyse those results.  Access to the server is via REST APIS which can be used to create your own data 
collection system.

* Code contributions are very welcome. 
* [Issue Tracker](https://github.com/smap-consulting/smapserver/issues)

Modules
-------
* surveyMobileAPI. Web services used by data collection clients
* surveyKPI.  Web services used for administration
* koboToolboxAPI.  Data APIs based initially on the API used by kobo toolbox
* subscribers.  A batch program whose main job is to apply submitted XML files to the database.
* sdDAL.  A library of shared code.
* sdDataAccess.  A legacy library of shared code.
* amazon. This is in the Smap2 repository, it provides access to AWS services.
* sms.  SMS and WhatsApp interface.

Other Projects
--------------

* setup.  Scripts used for installation and upgrading of a Smap server.
* codebook2.  A fork of the odk codebook for generating codebooks from survey templates.

Follow the latest news about Smap on our [blog](http://blog.smap.com.au)

Development
-----------

*  Install Eclipse IDE for Enterprise Java and Web
*  Install Java SDK 11
*  Clone this project
*  Clone the smap2 project which contains the amazon module: git clone https://github.com/nap2000/smap2.git
*  Import smapserver2 and smap2 as git repositories with sub projects
*  For each module select and set the project facets as: Dynamic Web Module 4.0, Java 11, JAX-RS 2.1.
*  For each module add java 11 as the JRE system library to the module path in the java build path libraries
*  sdDAL
    *  in java build path set the source folder to sdDAL/src
    *  Add amazon to projects/classpath in java build path
    *  Add amazon as a project in deployment assembly
    *  Add javarosa-[version].jar as a jar library under classpath.  It can be found in surveyKPI/src/main/webapp/WEB_INF/lib
*  sdDataAccess
    *  Add amazonand sdDAL to projects/classpath in java build path
    *  In deployment assemby set the deploy path of "/src" to "/"
*  koboToolboxApi
    *  Add sdDAL to projects/classpath in java build path 
    *  Add sdDAL as a project in deployment assembly
*  surveyMobileApi
    *  Add sdDAL, sdDataAccess to projects to build path and to deployment assembly
*  surveyKPI
    *  Add sdDAL, sdDataAccess, amazon to projects to build path and to deployment assembly
*  subscribers
    *  Add sdDAL, sdDataAccess, amazon to projects to build path
*  Run "maven update project" for all projects

Setting up a Test Environment
-----------------------------

If you are developing on one of the support Ubuntu LTS versions you can follow the [install instructions](https://www.smap.com.au/docs/server-admin-install.html).  However you can also manually set up a test environment on a linux or Mac system using the following steps. Use the install script as a guide.

*  Database  
    *  Install Postgresql
    *  Add a user ws with password ws1234
    *  create a database called "survey_dfinitions" and a database called "results"
    *  Add postgis and pgcrypto extensions to both
    *  Run the commands in setup/install/setupDb.sql in the survey_definitions database
    *  Run the commands in setup/install/resultsDb.sql in survey_definitions
*  Application Server
     *  Install Tomcat 9
     *  Add Tomcat as an Eclipse Server
     *  Copy the context.xml and server.xml files from the server tar to the eclipse tomcat server directory (uncomment a port in server.xml for http on 8080, this is needed by eclipse to check that the server started ok)
     *  Run surveyKPI, surveyMobileAPI and koboToolboxApi on the server
     
*  HTTP Web Server
     *  Install Apache Web Server
     *  Add apr utils and modules (refer to install script for details)
     *  Copy smap.conf, smap-ssl.conf, smap-volatile.conf to the apache2 sites-available directory
     *  Enable/disable apache sites
*  Files and Web Pages
      *  Download the server tar file
      *  Customise the deploy script to suit your installation and to copy files to smap_bin and the web site folder 
      *  Copy the jdbc driver to the tomcat lib directory
      *  Create the file structure at /smap to hold uploaded surveys etc
*  Subscriber
      *  There are two subscriber batch processors to run
      *  Run both as Java applications
      *  The main class is "Manager" for both
      *  [upload] Arguments are "default /smap upload"
      *  [forward] Arguments are "default /smap forward"
      
  
 

Upgrades
--------

*  Copy the updated war file or runnable jar file to the version1 folder
*  in the deploy folder run:
*    sudo ./patchdb.sh
*    sudo ./deploy.sh
