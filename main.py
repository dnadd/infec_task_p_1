import os
import bq_module, sql_module, db_handler

# Path for credentials files
ROOT_DIR = os.path.dirname(os.path.realpath(__file__))

############### PART 1 ###############

# Connect big query db
bqHandler = bq_module.BQHandler(ROOT_DIR + "/bt_credentials.json")

# Load db connection wrapper
dbHandler = db_handler.DBHandler()

# Load sql handler, and inject connection wrapper
sqlhandler = sql_module.SQLHandler(dbHandler, ROOT_DIR + '/db_credentials.json')

# Build db
sqlhandler.initSchema()

# Build the temperates data tables
sqlhandler.buildTemperatureTables()

# Pull Temperatures aggregations from Google bigquery
data = bqHandler.maxMinTemps()

# Parse and insert data
sqlhandler.insertTemperaturesData(data)


