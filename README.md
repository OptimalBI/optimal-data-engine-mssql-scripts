# Optimal Data Engine (ODE) Scripts #
Copyright 2015 OptimalBI - Licensed under the GNU GENERAL PUBLIC LICENSE Version 3.0 (GPL-3.0)

## What Optimal Data Engine Scripts does: ##
This solution contains a collection of scripts and SSIS packages, which you are free to use, at your own risk, to make working with ODE Configuration easier and more standard.

## Requirements (All users): ##
It is assumed that you have an ODE installation as these scripts all relate to ODE configuration.
ODE itself can be downloaded from the related project - "optimal-data-engine-mssql", which has it's own installation instructions. 

## Branches: ##
Currently, ODE Scripts has two Branches available:
* master and
* develop

Master contains code which is known to be running in a production environment.

Develop contains the latest, locally tested version of the codebase.

## Download and build instructions: ##
If you wish to develop the code base further, we recommend that you use the Visual Studio solution which is provided.

The Project contains a number of discrete scripts and SSIS packages, which you can copy off the GitHub website, or from the project zip.

### Pre-requisites ###

* Download a copy of the zip and extract to a temporary folder

### Installation ###

Admin database with "helper" stored procedures scripted installation:
* Open SQL Server Management studio and load *ode_to_mssql_scripts_Create.sql* from the extracted zip file. You will find it in the *ReleaseScript* folder.
* Within SQL Server Management Studio > Click Query Menu > SQLCMD Mode 
* Within the script optionally change the ConfigDatabase to your ODE_Config database name, DatabaseName and DefaultFilePrefix to the preferred ODE scripts database name; default is *ode_to_mssql_scripts*. *ODE_Admin* is recommended. 
* Click Execute from the toolbar. This should run successfully with a result of 'Update complete' on the Message panel

Admin database installation via Visual Studio:
* Open the solution in Visual Studio 2015
* Right-click on the *ode_to_mssql_scripts* project in the Solution Explorer, choose "Publish"
* Provide a server connection, desired database name and ODE_Config database name as a parameter.
* Click Publish, this will create a database with "helper" scripts

Release Management SSIS Project:
* Refer to the Release Management project ReadMe file

ODE_Calendar project:
* Refer to the ODE_Calendar project ReadMe file

ODE_Stage and ODE_Vault projects:
* Don't require installation as such. They could be used as a template for the ODE Data Vault implementation

## Current functionality: ##
Details of the current ODE functionality can be found here http://www.ode.ninja/category/features/
These scripts are linked on a number of pages in http://www.ode.ninja/ , where there usage is dicussed in more detail.

## Notes ##
* Untested on SQL Server editions prior to 2014. Installation script is compiled for SQL Server 2016.
* Stored procedures have hidden settings for columnstore indexes and table compression flags. You may need to edit stored procedures in case these features are not available in your instance of SQL Server.

## Feedback, suggestions, bugs, contributions: ##
Please submit these to GitHub issue tracking or join us in developing by forking the project and then making a pull request!

## Find out more: ##
Visit http://www.ode.ninja/ - this is where we keep our guides and share our knowledge. To find out more about OptimalBI and the work we do visit http://www.optimalbi.com or check out our blogs at http://optimalbi.com/blog/tag/data-vault/ for all the latest on our Data Vault journey. If you want to get in touch, you can email us at hey@optimalbi.com

## Change log: ##
```
Build 005.002.001 on 20180124
	* Added ODE Calendar
Build 005.001.001 on 20170911
	* Scripts are upgraded to run on ODE version 5.1
	* Added scripts to create CDC satellite functions
	* Added sample/template of Stage and Data Vault databases
Build 004.001.001 on 20170301
	* Scripts are upgraded to run on ODE version 4.1
	* Some scripts are transformed to stored procedures to be stored in ODE admin database (not in core config)
	* Added configuration release management SSIS package
20160819 
	* Initial Build.
```
