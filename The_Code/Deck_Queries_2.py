# Author: Casey Betts, 2023
# This file contains the queries used to interrogate the tasking deck and output information on misprioritized orders

import arcpy
# import pandas as pd
import json

# from datetime import datetime
from math import floor 
from pathlib import Path



def reorder_fields(input_layer, number_of_fields_to_move, output_name):
    """
    Reorders fields in a feature class or table by moving a specified number of fields from the end to the beginning.

    :param input_layer: String, the name of the input feature class or table
    :param fields_to_move: Integer, the number of fields to move from the end to the beginning
    :return: String, the name of the new feature class or table with reordered fields
    """
    arcpy.env.overwriteOutput = True

    # Get all fields
    fields = arcpy.ListFields(input_layer)
    field_names = [field.name for field in fields if field.type not in ['OID', 
                                                                        'Geometry', 
                                                                        'tasking_priority', 
                                                                        'sap_customer_identifier', 
                                                                        'responsiveness_level', 
                                                                        'purchase_order_header', 
                                                                        'price_per_area'
                                                                        ]]

    # Determine the new field order
    fields_to_move = min(number_of_fields_to_move, len(field_names))  # Ensure we don't try to move more fields than exist
    new_order = ['tasking_priority', 'sap_customer_identifier', 'responsiveness_level', 'purchase_order_header', 'price_per_area'] + field_names[-number_of_fields_to_move:] + field_names[:-number_of_fields_to_move]

    # Create a FieldMappings object
    field_mappings = arcpy.FieldMappings()
    field_mappings.addTable(input_layer)

    # Create a new FieldMappings object with fields in the desired order
    new_field_mappings = arcpy.FieldMappings()
    for field_name in new_order:
        field_index = field_mappings.findFieldMapIndex(field_name)
        if field_index != -1:
            field_map = field_mappings.getFieldMap(field_index)
            new_field_mappings.addFieldMap(field_map)

    # Create a new feature class or table with the reordered fields
    arcpy.FeatureClassToFeatureClass_conversion(input_layer, arcpy.env.workspace, output_name, field_mapping=new_field_mappings)

    arcpy.AddMessage(f"New layer '{output_name}' created with reordered fields.")

def build_feature_class(active_orders_ufp, query_input, temp_output, temp_loc, new_column_input):
    """
    Creates a new feature class with the given name and location. Adds Rivedo columns.

    :param output_name: String, the name of the output feature class
    :param output_loc: String, the location of the output feature class
    """

    # Store the path of the new feature class
    rivedo_feature_class = temp_loc + "\\" + temp_output

    # Create a feature class of the orders layer
    arcpy.conversion.ExportFeatures(active_orders_ufp, rivedo_feature_class)
    
    for new_column in new_column_input:
        
        # Open the code block file and save to var
        with open(new_column_input[new_column]['column_function'], 'r') as data:
            column_function = data.read() 

        # Add column to feature class for new priority
        field_name = new_column_input[new_column]["field_name"]
        expression = new_column_input[new_column]["expression"]
        code_block = "query_input =" + str(query_input) + "\n" + column_function
        field_type = new_column_input[new_column]["field_type"]
        arcpy.management.CalculateField(rivedo_feature_class, field_name, expression, "PYTHON3", code_block, field_type)

        
def get_layer_by_name(layer_name, map):
    """
    Returns the first layer in the TOC of the given name

    :param layer_name: String, the name of the layer to be returned
    """

    # Find the layer
    for layer in map.listLayers():
        if layer.name == layer_name:
            return layer
    else:
        raise Exception(f"Source layer '{layer_name}' not found in the TOC.")
    
    
def shape_output(temp_output, output_loc, excluded_priorities):
    """Temporary function to create a feature class and add it to the map """

    final_output = "Rivedo_orders"

    # Get the active map document and data frame
    project = arcpy.mp.ArcGISProject("CURRENT")
    map = project.activeMap

    # Add the feature layer to the map
    map.addDataFromPath(output_loc + "\\" + temp_output)

    # Generate a new layer with the new columns moved to the front
    reorder_fields(temp_output, 3, final_output)

    # Add the feature layer to the map
    map.addDataFromPath(output_loc + "\\" + final_output)

    # Remove the temp layer from the map
    map.removeLayer(get_layer_by_name(temp_output, map))

    # Select the desired rows
    rivedo_where_clause = "tasking_priority <> Rivedo_Pri Or Middle_Digit = 'Low' Or Middle_Digit = 'High' "
    arcpy.management.SelectLayerByAttribute(final_output, "NEW_SELECTION", rivedo_where_clause)
    arcpy.management.SelectLayerByAttribute(final_output, "SWITCH_SELECTION")
    exclusion_string = "(" + ",".join(str(num) for num in excluded_priorities) + ")"
    exlusion_where_clause = "tasking_priority IN" + exclusion_string
    arcpy.management.SelectLayerByAttribute(final_output, "ADD_TO_SELECTION", exlusion_where_clause)
    arcpy.management.DeleteFeatures(final_output)


def run(active_orders_ufp, path):
    """ This is the function called from the arc tool """

    # Define the path to the parameters and the output
    parameters_path = Path( path + r"\The_Code\Sensitive_Parameters.json")
    output_path = path + r"\Shapefile"

    # Load .json file with parameters
    with open(parameters_path, 'r', errors="ignore") as input:
        parameters = json.load(input)

    query_input = parameters["query_inputs"]
    excluded_priorities = parameters["excluded_priorities"]
    new_column_input = parameters["new_column_input"]

    temp_output = "Rivedo_temp"
    temp_loc = arcpy.env.workspace

    build_feature_class(active_orders_ufp, query_input, temp_output, temp_loc, new_column_input)

    shape_output(temp_output, temp_loc, excluded_priorities)
