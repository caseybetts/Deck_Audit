# Author: Casey Betts, 2023
# This file contains the functions needed to create a pandas dataframe from an ArcPro layer and return it

import arcpy
import pandas as pd

def create_dataframe(layer_name):
    """ Searches the map contents for the given layer and returns a dataframe from it """

    # Set variables to current project and map
    aprx = arcpy.mp.ArcGISProject("current")
    map = aprx.activeMap

    # Search layers for the active orders
    for layer in map.listLayers():
        if layer.isFeatureLayer:
            arcpy.AddMessage(layer.name)
            if layer.name == layer_name:
                break
    

    # Read the geo database table into pandas dataframe
    
    fields = [f.name for f in arcpy.ListFields(layer)]

    with arcpy.da.SearchCursor(layer, fields) as cursor:
        df = pd.DataFrame(list(cursor), columns=fields)

    return df
