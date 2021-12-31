[Smap smapserver](http://www.smap.com.au) 
======

The Smap Server manages survey definitions, stores submitted results and helps analyse those results.  This component contains the home page for the Smap server which has links to other components such as analysis and management.  It includes common files referenced by these other components.

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

Instructions on installing a Smap server can be found in the operations manual [here](http://www.smap.com.au/downloads.shtml)

Development
-----------
* Code contributions are very welcome. 
* [Issue Tracker](https://github.com/smap-consulting/smapserver/issues)

How to Install
--------------
