# hotswap mysql table

To alter a mysql table with minimal downtime.

Assumptions : 

	- id is the auto_increment column
	- indexes present in both id and autoUpdateCol separately
	- exception handling can be better; As DDL cannot be reverted/rollbacked
	- works for addition of column, changing the columns meta, adding/removing indices
	- fails in case of dropping a column ( insert into temp table will fail )
