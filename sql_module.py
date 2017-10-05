import json

class SQLHandler :

	def __init__(self, dbHandler, debCredentialsPath) :	
		self.dbHandler = dbHandler
		self.debCredentialsPath = debCredentialsPath

		# Get destination db creds
		with open(self.debCredentialsPath) as f :
			self.dbCredentials = json.load(f)

		# Connect db wrapper
		self.dbHandler.connect(self.dbCredentials["host"], "", self.dbCredentials["user"], self.dbCredentials["password"])

	def initSchema(self) : 
		# Refresh output db schema
		self.dbHandler.execute("DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;" % (self.dbCredentials["database"], self.dbCredentials["database"]))

		# Reconnect to new schema
		self.dbHandler.connect(self.dbCredentials["host"], self.dbCredentials["database"], self.dbCredentials["user"], self.dbCredentials["password"])

	def buildTemperatureTables(self) :
		# Break state and dates into lookup tables as date/strings are repeated across many rows

		# Dates lookup table
		sqlStr = "DROP TABLE IF EXISTS dates;"
		# Execute
		sqlResult = self.dbHandler.execute(sqlStr)
		if sqlResult[0] == False :
			raise IOError(sqlResult[1]) # Should use a more specific error type here relating to db
		# Create
		sqlStr = """
					CREATE TABLE dates (
						id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
						`date` DATE NOT NULL UNIQUE,
						INDEX (`date`)
					) ENGINE = InnoDB AUTO_INCREMENT = 0 DEFAULT CHARSET = utf8;
				"""
		# Execute
		sqlResult = self.dbHandler.execute(sqlStr)
		if sqlResult[0] == False :
			raise IOError(sqlResult[1]) # Should use a more specific error type here relating to db

		# States lookup table
		sqlStr = "DROP TABLE IF EXISTS states;"
		# Execute
		sqlResult = self.dbHandler.execute(sqlStr)
		if sqlResult[0] == False :
			raise IOError(sqlResult[1]) # Should use a more specific error type here relating to db
		# Create
		sqlStr = """
					CREATE TABLE states (
						id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
						state VARCHAR(5) NOT NULL UNIQUE,
						INDEX (state)
					) ENGINE = InnoDB AUTO_INCREMENT = 0 DEFAULT CHARSET = utf8;
				"""
		# Execute
		sqlResult = self.dbHandler.execute(sqlStr)
		if sqlResult[0] == False :
			raise IOError(sqlResult[1]) # Should use a more specific error type here relating to db

		# Build main temperatures table
		sqlStr = "DROP TABLE IF EXISTS temperatures;"
		# Execute
		sqlResult = self.dbHandler.execute(sqlStr)
		if sqlResult[0] == False :
			raise IOError(sqlResult[1]) # Should use a more specific error type here relating to db
		sqlStr = """
					CREATE TABLE temperatures (
						id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
						temp_max FLOAT(5, 2) NOT NULL,
						temp_min FLOAT(5, 2) NOT NULL,
						date_id INT UNSIGNED NOT NULL,				
						state_id INT UNSIGNED NOT NULL,	
						FOREIGN KEY (state_id) REFERENCES states(id) ON DELETE CASCADE,	
						FOREIGN KEY (date_id) REFERENCES dates(id) ON DELETE CASCADE,				
						INDEX(temp_max),
						INDEX(temp_min),						
						INDEX(date_id),	
						INDEX(state_id),
						UNIQUE(state_id, date_id)
					) ENGINE = InnoDB AUTO_INCREMENT = 0 DEFAULT CHARSET = utf8;
				"""
		# Execute
		sqlResult = self.dbHandler.execute(sqlStr)
		if sqlResult[0] == False :
			raise IOError(sqlResult[1]) # Should use a more specific error type here relating to db

	def insertTemperaturesData(self, data) : 
		dates = set([])
		states = set([])

		# First, pull unique dates and states
		for row in data :
			dateStr = "%s-%s-%s" % (row["year"], row["mo"], row["da"])
			if dateStr not in dates :
				dates.add(dateStr)
			if row["state"] not in states :
				states.add(row["state"])	

		# Insert into lookup tables
		# States
		args = []
		sqlStr = "INSERT IGNORE INTO states (state) VALUES "
		statesLookupSqlInStr = ""
		first = True
		for state in states :
			if first == True :
				first = False 
			else :
				sqlStr += ","
			sqlStr += "(%s)"
			args.append(state)
		# Execute
		if len(args) > 0 :
			# Execute
			sqlResult = self.dbHandler.execute(sqlStr, args)
			if sqlResult[0] == False :
				raise IOError(sqlResult[1]) # Should use a more specific error type here relating to db

		# Insert into lookup tables
		# Dates
		args = []
		sqlStr = "INSERT IGNORE INTO dates (`date`) VALUES "
		first = True
		for date in dates :
			if first == True :
				first = False 
			else :
				sqlStr += ","
			sqlStr += "(%s)"
			args.append(date)
		# Execute
		if len(args) > 0 :
			# Execute
			sqlResult = self.dbHandler.execute(sqlStr, args)
			if sqlResult[0] == False :
				raise IOError(sqlResult[1]) # Should use a more specific error type here relating to db

		# Pull lookups

		# States
		statesIdsLookup = {}
		args = []
		sqlStr = "SELECT * FROM states WHERE state IN ("
		first = True 
		for state in states :
			if first == True :
				first = False 
			else :
				sqlStr += ","
			sqlStr += "%s"
			args.append(state)
		sqlStr += ");"
		rs = self.dbHandler.select(sqlStr, args)
		for row in rs :
			statesIdsLookup[row["state"]] = row["id"]

		# Dates
		datesIdsLookup = {}
		args = []
		sqlStr = "SELECT * FROM dates WHERE `date` IN ("
		first = True 
		for date in dates :
			if first == True :
				first = False 
			else :
				sqlStr += ","
			sqlStr += "%s"
			args.append(date)
		sqlStr += ");"
		rs = self.dbHandler.select(sqlStr, args)
		for row in rs :
			dateStr = row["date"].strftime("%Y-%m-%d")
			datesIdsLookup[dateStr] = row["id"]

		# Do main insert - would usually limit the length of this to batches of a few hundred thousand so not to run out of memory
		args = []
		sqlStr = "INSERT INTO temperatures (temp_max, temp_min, date_id, state_id) VALUES "
		first = True
		for row in data :
			state = row["state"]
			dateStr = "%s-%s-%s" % (row["year"], row["mo"], row["da"])
			# Check we have the lookup ids, skip if not
			if state not in statesIdsLookup or dateStr not in datesIdsLookup :
				continue
			# Check for comma
			if first == True :
				first = False 
			else :
				sqlStr += ","
			sqlStr += "(%s, %s, %s, %s)"
			args.append(self.toCelsius(row["temp_max"]))
			args.append(self.toCelsius(row["temp_min"]))
			args.append(datesIdsLookup[dateStr])
			args.append(statesIdsLookup[state])			

		# Do overwrite if duplicate state/date combos
		sqlStr += " ON DUPLICATE KEY UPDATE temp_max = VALUES(temp_max), temp_min = VALUES(temp_min);"

		# Default result if nothing gets added
		resultRowsAdded = 0

		# Execute
		if len(args) > 0 :
			# Execute
			sqlResult = self.dbHandler.execute(sqlStr, args)
			if sqlResult[0] == False :
				raise IOError(sqlResult[1]) # Should use a more specific error type here relating to db
			# return this final sql result
			resultRowsAdded = sqlResult[2]

		# Done
		return resultRowsAdded

	# This would not be in here usually - would be in an injected maths/utils library
	def toCelsius(self, fahrenheit) :
		return (fahrenheit - 32) / 1.8