""" 
    Author: Casey Betts, 2023
    This file contains the function to return a pandas dataframe of a given ArcPro layer
    Note: This script does not support nested layers, so the target layer must not be nested in a group layer 
"""

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

            if layer.name == layer_name:
                break

    # Read the geo database table into pandas dataframe
    fields = [f.name for f in arcpy.ListFields(layer)]

    with arcpy.da.SearchCursor(layer, fields) as cursor:
        df = pd.DataFrame(list(cursor), columns=fields)

    return df
