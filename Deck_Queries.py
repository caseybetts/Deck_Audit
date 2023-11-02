# This file contains the queries used to interrogate the tasking deck and will return the results



import pandas as pd
import arcpy

# Get layer object
layer = arcpy.GetParameterAsText(0) 

# Get a list of field names
fields = [f.name for f in arcpy.ListFields(layer)]

# Read layer into dataframe
with arcpy.da.SearchCursor(layer, fields) as cursor:
    data = [[r[0] for r in cursor.fields] + list(cursor)]
    df = pd.DataFrame(data[1:], columns=data[0])

print(df.head())                     















      



