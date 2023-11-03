# This file contains the queries used to interrogate the tasking deck and will return the results

import arcpy
import pandas as pd
# Set variables to current project and map
current_project = arcpy.mp.ArcGISProject("current")
current_map = current_project.activeMap

# Search layers for the active orders
for layer in current_map.listLayers():
    if layer.isFeatureLayer:
        if layer.name == 'PROD_Active_Orders_UFP':
            active_orders_layer = layer
            break

# Read the geo database table into pandas dataframe
fields = [f.name for f in arcpy.ListFields(layer)]

with arcpy.da.SearchCursor(layer, fields) as cursor:
    df = pd.DataFrame(list(cursor), columns=fields)

print(df.head())         