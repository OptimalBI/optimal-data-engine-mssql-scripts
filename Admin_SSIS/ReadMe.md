#ODE Admin SSIS project#

ODE SSIS Admin project is a set of SSIS packages for perform administration tasks for ODE instance. 
At the moment project consists of two packages.
* Create_access_functions contains a logic to create or rebuild all the access functions for every satellite in the config. Package uses an Admin procedure for rebuilding table functions from the core ODE scripts project and require them to be installed on server. Use this SSIS package for the bulk rebuild during the development. You also can add it to the SQL Agent job to be excuted on particular schedule to reflect all the changes made to the Vault. Rad more about access functions: http://ode.ninja/satellite-change-data-capture-functions/
* Restart_Schedule_single is an easy way to restart the failed load after it has been fixed. Once installed on server, you can execute the package from the Integration services catalog with the parameter of the failed load. Read more about task scheduling here: http://ode.ninja/task-scheduling/

## Installation ##
* Open the project in Visual Studio 2015. 
* In the Solution Explorer edit project's connections under the project's Connection Managers folder. Connections should both point to the server where ODE is installed and where you are planning to deploy the project. Connection ODE_Config should point to your instance of ODE Config database. Connection ODE_Admin should point to your instance of ODE Admin database.
* Right-click on the project in the Solutio Explorer, choose Deploy. Provide a server name and a folder in the Integration Services catalog where the project will be installed.

## Notes ##
* SSIS package has been implemented in Visual Studio 2015 for the target SQL Server 2016.

#Execution#

Right click on the package in the SQL Server Integration services catalog, choose "Execute". 
You will need to provide a parameter for the Restart_Schedule_single package. Package Create_access_functions is executed without parameters.
In case of failure, follow the links on the report to open all messages report for the error message.

## Feedback, suggestions, bugs, contributions: ##
Please submit these to GitHub issue tracking or join us in developing by forking the project and then making a pull request!

## Find out more: ##
Visit http://ode.ninja/ - this is where we keep our guides and share our knowledge. To find out more about OptimalBI and the work we do visit http://www.optimalbi.com or check out our blogs at http://optimalbi.com/blog/tag/data-vault/ for all the latest on our Data Vault journey. If you want to get in touch, you can email us at hey@optimalbi.com
