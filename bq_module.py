import uuid
import os
from google.cloud import bigquery

class BQHandler : 

    def __init__(self, credentialsPath) :        
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentialsPath

    def maxMinTemps(self, yearMin = 1990, yearMax = 2000):
        # Check input
        if yearMin >= yearMax:
            raise ValueError("Invalid date range given")

        client = bigquery.Client()

        # Build the range of years we want
        yearRange = list(range(yearMin, yearMax + 1))

        # Base table string for dynamic range
        baseTableStr = "[bigquery-public-data:noaa_gsod.gsod%d]"

        # Build list of tables to union
        tablesList = ""
        first = True
        for year in yearRange :
            if first == True :
                first = False
            else :
                tablesList += ","
            # insert year
            tablesList += baseTableStr % year

        # Sub query
        subSqlStr = """
            (
                SELECT
                stn AS stn,
                max AS max,
                min AS min,
                da AS da,
                mo AS mo,
                year AS year
                FROM
                %s 
                WHERE min < 9999.9 AND max < 9999.9
            )
        """

        # Main query
        mainSqlStr = """
            SELECT
            MAX(temps_table.max) AS temp_max,
            MIN(temps_table.min) AS temp_min,
            temps_table.da AS da,
            temps_table.mo AS mo,
            temps_table.year AS year,
            station_table.state AS state
            FROM  %s  AS temps_table
            INNER JOIN [bigquery-public-data:noaa_gsod.stations] AS station_table ON temps_table.stn = station_table.usaf
            WHERE station_table.state != ''
            GROUP BY state, year, mo, da
            ORDER BY state, year, mo, da 
        """

        # Merge into one query
        sqlStr = mainSqlStr % (subSqlStr % tablesList)
       
        # Build job
        job = client.run_async_query(str(uuid.uuid4()), query = sqlStr) 

        job.begin()
        job.result() 

        result = []

        # Pull
        resultsTable = job.destination
        resultsTable.reload()

        # Add to results list
        for row in resultsTable.fetch_data():
            result.append({
                "temp_max" : row[0],
                "temp_min" : row[1],
                "da" : row[2],        
                "mo" : row[3], 
                "year" : row[4], 
                "state" : row[5],             
            })

        # Done
        return result
