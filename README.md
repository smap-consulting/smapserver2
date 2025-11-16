# [Smap Server](http://www.smap.com.au) 

The Smap Server manages survey definitions, stores submitted results and helps analyse those results.  Access to the server is via REST APIS which can be used to create your own data 
collection system.

* Code contributions are very welcome. 
* [Issue Tracker](https://github.com/smap-consulting/smapserver2/issues)

## Modules

* surveyMobileAPI. Web services used by data collection clients
* surveyKPI.  Web services used for administration
* koboToolboxAPI.  Data APIs based initially on the API used by kobo toolbox
* subscribers.  A batch program whose main job is to apply submitted XML files to the database.

##### Shared Libraries

* sdDAL.  Access to the database.
* sdDataAccess.  A legacy library to access the database.
* amazon. This is in the Smap2 repository, it provides access to AWS services.
* sms.  SMS and WhatsApp interface.

##### Other Projects

* setup.  Scripts used for installation and upgrading of a Smap server.
* codebook2.  A fork of the odk codebook for generating codebooks from survey templates.

Follow the latest news about Smap on our [blog](http://blog.smap.com.au)

## Development

*  Create a directory called "deploy" under your home directory, build scripts will put their output here
*  Install Java SDK 11
*  Clone this project
*  Clone the smap2 project which contains the amazon module: git clone https://github.com/nap2000/smap2.git
* sdDAL
    * Add javarosa-[version].jar as a maven jar library under classpath.  It can be found in surveyKPI/src/main/webapp/WEB_INF/lib. To add to maven:
    * mvn org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file -Dfile=../surveyKPI/src/main/webapp/WEB-INF/lib/javarosa-3.1.4.jar -DgroupId=smapserver -DartifactId=javarosa -Dversion=3.1.4 -Dpackaging=jar

Smap is a web application and requires enterprise java.  We recommend using Apache Netbeans as an IDE due to its ease of use however instructions for setting up Eclipse are also included here.

### Apache Netbeans IDE

*  Set each project as a maven project: amazon, sdDAL, sdDataAccess, surveyMobileAPI, surveyKPI, koboToolboxApi
*  Run mvn clean install for all projects


### Eclipse

*  Install Eclipse IDE for Enterprise Java and Web
*  Import smapserver2 and smap2 as git repositories with sub projects
*  For each module select and set the project facets as: Dynamic Web Module 4.0, Java 11, JAX-RS 2.1.
*  For each module add java 11 as the JRE system library to the module path in the java build path libraries
*  sdDAL
    *  in java build path set the source folder to sdDAL/src
    *  Add amazon to projects/classpath in java build path
    *  Add amazon as a project in deployment assembly
*  sdDataAccess
    *  Add sdDAL to projects/classpath in java build path
    *  In deployment assembly, set the deploy path of "/src" to "/"
*  Amazon
*  koboToolboxApi
    *  Add sdDAL to projects/classpath in java build path 
    *  Add sdDAL as a project in deployment assembly
*  surveyMobileApi
    *  Add sdDAL, sdDataAccess to projects to build path and to deployment assembly
*  surveyKPI
    *  Add sdDAL, sdDataAccess, amazon to projects to build path and to deployment assembly
*  subscribers
    *  Add sdDAL, sdDataAccess, amazon to projects to build path
*  **Run "maven update project" for all projects**

##### Build and Deployment

*  Use the eclipse export command to export surveyKPI, surveyMobileApi and koboToolboxApi
*  Run the ant build file subscriber3.xml in subscriber to create a runnable jar file in the deploy directory under home
*  Run the dep.sh script in smapserver2/setup to create a directory called "smap" in the deploy directory containing the install and deploy scripts for a Smap Server
*  Copy the war files and runnable jar files into deploy/smap/deploy/version1
*  The deploy/smap directory can then be deployed to a server to install smap server or update an existing installation

## Setting up a Test Environment


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
     *  Add Tomcat as an server in your IDE
     *  Edit the context.xml file in the tomcat conf directory.  Add the two resources "jdbc/survey_dfinitions" and "jdbc/results" from the source context.xml file "$HOME/git/smapserver2/setup/install/config_files/context.xml" 
     *  Edit the server.xml file to open up the 8009 port.  Use the source server.xml file from "$HOME/git/smapserver2/setup/install/config_files/server.xml.tomcat9" as a template.   
*  Run surveyKPI, surveyMobileAPI and koboToolboxApi on the server
     
*  HTTP Web Server
     *  Install Apache Web Server
     *  Add apr utils and modules (refer to install script for details)
     *  Copy smap.conf, smap-ssl.conf, smap-volatile.conf to the apache2 sites-available directory
     *  Enable/disable apache sites
*  Files and Web Pages
      *  Download the server tar file
      *  Customise the deploy script to suit your installation and to copy files to smap_bin and the web site folder 
      *  Copy the JDBC driver to the tomcat lib directory
      *  Create the file structure at /smap to hold uploaded surveys etc
*  Subscriber
      *  There are two subscriber batch processors to run
      *  Run both as Java applications
      *  The main class is "Manager" for both
      *  [upload] Arguments are "default /smap upload"
      *  [forward] Arguments are "default /smap forward"


