# hotswap_mysql_table

To alter a mysql table with minimal downtime

Assumptions : 

	- id is the auto_increment column
	- indexes present in both id and autoUpdateCol separately
	- exception handling can be better; As DDL cannot be reverted/rollbacked
