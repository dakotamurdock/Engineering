'''
Script to query SQL database to get wells and BHA info that meet hole size requirements and then query MongoDB
performanceBHA collection to retrieve performance numbers for those BHAs. Results of these queries are merged
and exported to CSV for SME analysis.
'''

import pandas as pd
from pandas import json_normalize
import pyodbc as sql
from pymongo import MongoClient

# Connecting to SQL Server
cnxn = sql.connect(DRIVER='{SQL Server}',
                   Server='redacted',
                   Database='redacted',
                   uid='redacted',
                   pwd='redacted')

# Get input in console for upper and lower bounds of hole sizes they are looking for
hole_size_upper = input("Enter the hole size upper bound with two decimal places.")
hole_size_lower = input("Enter the hole size lower bound with two decimal places.")

# SQL query being sent to SQL Server including upper and lower hole size bounds from user
query = 'SELECT TOP 1000 [bha].[id] AS [bha_id],'\
            '[bha].[holeSize],'\
            '[well].[WB_NAMEWELL] AS [well_name],'\
            '[well].[UIDWELL] AS [well_uid]'\
        'FROM [dbo].[wb_wellbore] AS [well]'\
        'INNER JOIN [dbo].[bha] AS [bha]'\
            'ON [well].[UIDWELL] = [bha].[wellUid]'\
        f'WHERE [bha].[holeSize] < {hole_size_upper} AND [bha].[holeSize] > {hole_size_lower}'\
        'ORDER BY [well].[updated_at] DESC'

# Stores response in Pandas DataFrame
sql_response_df = pd.read_sql(query, cnxn)

# Taking the wellUid field from SQL and making it a list to use in the MongoDB query by UID
uid_series = sql_response_df.well_uid.unique()
# uid_series = uid_series[:5]  # Limiting to just first 5 results for testing
uid_list = uid_series.tolist()

# Connecting to MongoDB
client = MongoClient('mongodb://redacted/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')
db = client.t3
collection = db.performanceBHA

# Filter to just the documents with the uid's returned from the SQL query
mongo_response = collection.find({'uid': {'$in': uid_list}})

# Converting the JSON response into a Pandas DataFrame
mongo_response_df = json_normalize(mongo_response, record_path=['bha'])

# SQL style join of two DataFrames on bha id
bha_result_df = pd.merge(sql_response_df, mongo_response_df, left_on="bha_id", right_on="_id", how="inner")

# Rearranging, limiting, and renaming columns to prepare for export
cols = [
    'well_name',
    'well_uid',
    'name',
    'holeSize',
    'mdStart',
    'mdEnd',
    'sections',
    'footageDrilled.section.all',
    'avgRop.section.all',
    'rotatingROP.section.all',
    'slidingROP.section.all',
    'effectiveROP.section.all',
    'slidePercentage.section.all',
    'footageDrilled.section.surface',
    'avgRop.section.surface',
    'rotatingROP.section.surface',
    'slidingROP.section.surface',
    'effectiveROP.section.surface',
    'slidePercentage.section.surface',
    'footageDrilled.section.intermediate',
    'avgRop.section.intermediate',
    'rotatingROP.section.intermediate',
    'slidingROP.section.intermediate',
    'effectiveROP.section.intermediate',
    'slidePercentage.section.intermediate',
    'footageDrilled.section.curve',
    'avgRop.section.curve',
    'rotatingROP.section.curve',
    'slidingROP.section.curve',
    'effectiveROP.section.curve',
    'slidePercentage.section.curve',
    'footageDrilled.section.lateral',
    'avgRop.section.lateral',
    'rotatingROP.section.lateral',
    'slidingROP.section.lateral',
    'effectiveROP.section.lateral',
    'slidePercentage.section.lateral'
]
rename_cols = {
    'well_name': "Well Name",
    'well_uid': "Well ID",
    'name': "BHA Name",
    'holeSize': "Hole Size",
    'mdStart': "Start Depth (MD)",
    'mdEnd': "End Depth (MD)",
    'sections': "Sections Drilled",
    'footageDrilled.section.all': "Total Footage Drilled",
    'avgRop.section.all': "Overall Average ROP",
    'rotatingROP.section.all': "Overall Rotating ROP",
    'slidingROP.section.all': "Overall Sliding ROP",
    'effectiveROP.section.all': "Overall Effective ROP",
    'slidePercentage.section.all': "Overall Sliding Percentage",
    'footageDrilled.section.surface': "Surface Footage Drilled",
    'avgRop.section.surface': "Surface Average ROP",
    'rotatingROP.section.surface': "Surface Rotating ROP",
    'slidingROP.section.surface': "Surface Sliding ROP",
    'effectiveROP.section.surface': "Surface Effective ROP",
    'slidePercentage.section.surface': "Surface Sliding Percentage",
    'footageDrilled.section.intermediate': "Intermediate Footage Drilled",
    'avgRop.section.intermediate': "Intermediate Average ROP",
    'rotatingROP.section.intermediate': "Intermediate Rotating ROP",
    'slidingROP.section.intermediate': "Intermediate Sliding ROP",
    'effectiveROP.section.intermediate': "Intermediate Effective ROP",
    'slidePercentage.section.intermediate': "Intermediate Sliding Percentage",
    'footageDrilled.section.curve': "Curve Footage Drilled",
    'avgRop.section.curve': "Curve Average ROP",
    'rotatingROP.section.curve': "Curve Rotating ROP",
    'slidingROP.section.curve': "Curve Sliding ROP",
    'effectiveROP.section.curve': "Curve Effective ROP",
    'slidePercentage.section.curve': "Curve Sliding Percentage",
    'footageDrilled.section.lateral': "Lateral Footage Drilled",
    'avgRop.section.lateral': "Lateral Average ROP",
    'rotatingROP.section.lateral': "Lateral Rotating ROP",
    'slidingROP.section.lateral': "Lateral Sliding ROP",
    'effectiveROP.section.lateral': "Lateral Effective ROP",
    'slidePercentage.section.lateral': "Lateral Sliding Percentage"
}

bha_result_df = bha_result_df[cols].rename(columns=rename_cols)

# Exporting results to CSV file on Desktop for SME usage
bha_result_df.to_csv(path_or_buf='C:\\Users\\DakotaMurdock\\Desktop\\bha_result.csv', index=False)
