# ODE Calendar project #

Simple calendar ensemble for Data Vaults created with ODE. The concept is similar to Date (calendar) dimension in Star schema data warehouses.
Ensemble grain is date. Data is generated using the stored procedures logic. 
Includes New Zealand local holidays.

## Installation ##
* Find file ODE_Calendar_Create_script_Create.sql in Calendar_Release_Script folder, open it with SQL Server Management Studio.
* Connect to the server where you have your copy of ODE installed and change query mode to SQL cmd (Query -> SQLCMD Mode).
* Change default variables to match your implementation. 
		Change the value of variable ODE_Config to your ODE config database name.
		Change the value of variable ODE_Vault to your Data Vault database name.
		Change the value of variable DatabaseName to your Stage database name.
* Execute query
* Add Calendar ensembles to your Daily data load schedule or just execute the following to populate Calendar with the data (it takes a few minutes):
```sql
EXECUTE [ODE_Config].[dv_scheduler].[dv_process_schedule] 'Load_Calendar'
```

## Notes ##

With the current implementation, you only can have one Calendar installed over multiple Data Vaults managed by one ODE Config instance. Use Calendar view to access Calendar from other Vaults.
By default Calendar view will be installed in Data Vault database. 
Use Calendar view if you need to use holidays data. As there could be multiple holidays in one day, the grain of Calendar and Holidays ensembles is different.
Don't forget to execute the data load to populate the Calendar. By default Calendar is populated with the data for 10 years in advance. Edit stage stored procedures, change the value of @YearsInAdvance variable to the desired number of years in advance if you need more future dates in your calendar. If scheduled, on 1st January Calendar populates with the data for the new tenth year in advance. Re-run the process schedule procedure every time you have edited stored procedures.
ODE require unique source table name to exist. Script will fail if you already have tables Calendar, CalendarFiscal or CalendarHolidays configured.

## Feedback, suggestions, bugs, contributions: ##
Please submit these to GitHub issue tracking or join us in developing by forking the project and then making a pull request!

## Find out more: ##
Visit http://ode.ninja/ - this is where we keep our guides and share our knowledge. To find out more about OptimalBI and the work we do visit http://www.optimalbi.com or check out our blogs at http://optimalbi.com/blog/tag/data-vault/ for all the latest on our Data Vault journey. If you want to get in touch, you can email us at hey@optimalbi.com
