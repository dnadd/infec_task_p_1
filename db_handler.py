import pymysql

class DBHandler() :
	# Attempt DB Connect
	def connect(self, host, dbName, username, password) :
		message = "Connection success"
		result = False
		try :
			self.conn = pymysql.connect(host = host, user = username, passwd = password, db = dbName)
			result = True
		# TODO catch proper error not all
		except pymysql.InternalError as error :
			message = error.args
		# Return
		return(result, message)

	# Close the DB connection
	def closeConnection(self) :
		self.conn.close()

	def __del__(self):
		self.closeConnection()
	
	# Execute A statement, True if no errors
	def execute(self, sqlString, args = []) :
		# Cursor
		cur = self.conn.cursor()
		rows = 0
		result = True
		message = "Execution success"
		try :
			rows = cur.execute(sqlString, args)
			self.conn.commit()
		except pymysql.InternalError as error :
			message = str(error)
			result = False
		# Close cursor
		cur.close()
		# return
		return (result, message, rows)

	# Query and parse result set into list of maps
	def select(self, sqlString, args = []):
		# Cursor
		cur = self.conn.cursor()
		mysqlResult = cur.execute(sqlString, args)
		data = cur.fetchall()
		desc = cur.description
		result = []
		# Return list of maps of data
		for row in data :
			resultRow = {}
			for (name, value) in zip(desc, row):
			    resultRow[name[0]] = value
			result.append(resultRow)
		# Close cursor
		cur.close()
		return result