"""
To hotswap a table

Assumptions : 
	id is the auto_increment column
	indexes present in both id and autoUpdateCol separately
"""
import MySQLdb
import subprocess
import time

# thread run the queries from the file mention as many time requested in command line
def swap( dbname , tableName, sql , autoUpdateCol = "auto_update_timestamp" ):

	tempTable = "hotswap." + tableName 
	originalTable = dbname + "." + tableName
	username = "root"; password = "root"

	print "Starting to alter the table -> " + originalTable;

	try:
		con = MySQLdb.connect('localhost', username, password, dbname)
		cursor = con.cursor( MySQLdb.cursors.DictCursor )
		con.autocommit(0)

		print ("*"*20) + "PHASE 1 : Setting up new table " + ("*"*20)
		## create the new table with desired schema
		cursor.execute("CREATE DATABASE IF NOT EXISTS `hotswap`")		
		cursor.execute("CREATE TABLE IF NOT EXISTS " +tempTable + " LIKE " + originalTable)
		print "created new database and table ->" + tempTable

		alterSql = sql.replace("`" , "").replace(originalTable, tempTable, 1)
		cursor.execute(alterSql)
		print "Altered new table ->" + alterSql

		dumpFile = "/mnt/hotswap/"+ tempTable + ".sql"
		subprocess.call("mkdir -p /mnt/hotswap/", shell=True)
		print "Created file"

		print ("*"*20) + "PHASE 2 : Initial Dump ( longest step) " + ("*"*20)
		initialDump = "mysqldump -p"+ password +" -u "+ username+" " + originalTable.replace(".", " ", 1) \
		  + " --no-create-info --skip-tz-utc --complete-insert --compact --skip-comments --skip-lock-tables > " + dumpFile
		subprocess.call(initialDump, shell=True)
		print "Table dump is taken -> " + initialDump

		subprocess.call("mysql -p"+ password +" -u "+ username+" hotswap < " +  dumpFile,  shell=True)
		print "New table is restored " 

		#cursor.execute("SELECT NOW() into @temp_max_time")
		# assumption : too many insert, wchih implies the last record is almost the last write
		cursor.execute("SELECT id into @temp_max_id, "+autoUpdateCol+" into @temp_max_time from " + tempTable +" order by id desc limit 1")
		cursor.execute("SELECT @temp_max_id as max_id, @temp_max_time as max_time")
		max_values = cursor.fetchone()
		print "Max values in table are -> " + str(max_values["max_time"]) + "/" + str(max_values["max_id"])



		## delta insert; larger chunk as replace was taking long
		print ("*"*20) + "PHASE 3 : Delta Dump ( data changed since last dump) " + ("*"*20)
		deltaFile = "/mnt/hotswap/"+ tempTable + "_2.sql"
		secondaryDump = "mysqldump -p"+ password +" -u "+ username+" " + originalTable.replace(".", " ", 1) \
		  + " --no-create-info --skip-tz-utc --complete-insert --compact --skip-comments --skip-lock-tables --replace  " 
		subprocess.call(secondaryDump + " --where 'id > "+ str(max_values["max_id"]) + " '> " + deltaFile , shell=True)
		subprocess.call(secondaryDump + " --where '"+autoUpdateCol+" >  \""+ str(max_values["max_time"]) + "\"' >> " + deltaFile , shell=True)
		subprocess.call("mysql -p"+ password +" -u "+ username+" hotswap < " +  deltaFile,  shell=True)

		#cursor.execute("SELECT NOW() into @temp_max_time")
		cursor.execute("SELECT id into @temp_max_id , "+autoUpdateCol+" into @temp_max_time from " + tempTable +" order by id desc limit 1")
		cursor.execute("SELECT @temp_max_id as max_id, @temp_max_time as max_time")
		max_values = cursor.fetchone()
		print "After major delta Max values in table are -> " + str(max_values["max_time"]) + "/" + str(max_values["max_id"])
		cursor.execute("INSERT INTO " + tempTable + " (id) values ("+str(max_values["max_id"] + 10000)+")")

		print ("*"*20) + "PHASE 4 : Rename Table " + ("*"*20)
		oldTable = tempTable + "_" + str(int(time.time()))
		cursor.execute("RENAME TABLE  "+originalTable +" to  " + oldTable + ", "+ tempTable + " to "+ originalTable)
		print "Swapped the tables now"


		print ("*"*20) + "PHASE 5 : Delta dump (data changed phase 3) " + ("*"*20)
		deltaFile = "/mnt/hotswap/"+ tempTable + "_3.sql"
		subprocess.call(secondaryDump + " --where 'id > "+ str(max_values["max_id"]) + " '> " + deltaFile , shell=True)
		subprocess.call(secondaryDump + " --where '"+autoUpdateCol+" >  \""+ str(max_values["max_time"]) + "\" AND id < "+str(max_values["max_id"])+ "' >> " + deltaFile , shell=True)
		subprocess.call("mysql -p"+ password +" -u "+ username+" " + dbname +"  < " +  deltaFile,  shell=True)
		print "Voila!! You are done with altering the table"

		print ("*"*20) + "PHASE 6 : DO A SANITY " + ("*"*20)

	except MySQLdb.Error, e:
		print "Error %d: %s %s" % (time.time()-start, e.args[1], alterSql)
		sys.exit(1)
	finally:
		if con:
			con.close()


## main function starts here
if __name__ == '__main__':

	## the dbnames have the tables to run, all the queries are expected to run in all databases
	swap( "masters", "organizations", "alter table masters.organizations add column dummy int(11)", "auto_update_timestamp" )
