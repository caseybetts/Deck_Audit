# Author: Casey Betts, 2023
# This file contains the queries used to interrogate the tasking deck and output information on misprioritized orders

import arcpy
# import pandas as pd
import json

# from datetime import datetime
from math import floor 
from pathlib import Path



def produce_field_mapping(input_layer, number_of_fields_to_move, new_existing_field_mapping):
    """
    Creates a field mapping as input for creating a new feature class

    :param input_layer: String, the name of the input feature class or table
    :param fields_to_move: Integer, the number of fields to move from the end to the beginning
    :return: String, the name of the new feature class or table with reordered fields
    """
    arcpy.env.overwriteOutput = True

    # Get a list of all field names minus the existing ones that will be moved
    fields = arcpy.ListFields(input_layer)
    field_names = [field.name for field in fields if field.type not in ['OID', 'Geometry'] + new_existing_field_mapping]

    # Determine the new field order
    number_of_fields_to_move = min(number_of_fields_to_move, len(field_names))  # Ensure we don't try to move more fields than exist
    new_field_order = new_existing_field_mapping + field_names[-number_of_fields_to_move:] + field_names[:-number_of_fields_to_move]

    # Create a FieldMappings object
    field_mappings = arcpy.FieldMappings()
    field_mappings.addTable(input_layer)

    # Create a new FieldMappings object with fields in the desired order
    new_field_mappings = arcpy.FieldMappings()
    for field_name in new_field_order:
        field_index = field_mappings.findFieldMapIndex(field_name)
        if field_index != -1:
            field_map = field_mappings.getFieldMap(field_index)
            new_field_mappings.addFieldMap(field_map)

    return new_field_mappings


def add_columns_to_layer(feature_layer, query_input, new_column_input):
    """
    Adds the given columns to the given feature layer

    :param feature_layer: Feature Layer, the feature layer to have columns added
    :param query_input: Dict, specs used by the column functions
    :param new_column_input: Dict, column parameters to be used as input for the CalculateField funciton
    """

    for new_column in new_column_input:
        
        # Open the code block file and save to var
        with open(new_column_input[new_column]['column_function'], 'r') as data:
            column_function = data.read()

        # Add column to feature class for new priority
        field_name = new_column_input[new_column]["field_name"]
        expression = new_column_input[new_column]["expression"]
        code_block = "query_input =" + str(query_input) + "\n" + column_function
        field_type = new_column_input[new_column]["field_type"]
        arcpy.management.CalculateField(feature_layer, field_name, expression, "PYTHON3", code_block, field_type)

        
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
    
    
def shape_output(temp_output, output_loc, excluded_priorities, new_existing_field_mapping):
    """Temporary function to create a feature class and add it to the map """

    final_output = "Rivedo_orders"

    # Get the active map document and data frame
    project = arcpy.mp.ArcGISProject("CURRENT")
    map = project.activeMap

    # Add the feature layer to the map
    map.addDataFromPath(output_loc + "\\" + temp_output)

    # Generate a new layer with the new columns moved to the front
    new_field_mapping = produce_field_mapping(temp_output, 3, new_existing_field_mapping)

    # Create a new feature class with the reordered fields
    arcpy.FeatureClassToFeatureClass_conversion(temp_output, arcpy.env.workspace, final_output, field_mapping = new_field_mapping)

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
        config = json.load(input)

    temp_output = "Rivedo_temp"
    temp_loc = arcpy.env.workspace

    # Store the path of the new feature class
    rivedo_feature_class = temp_loc + "\\" + temp_output

    # Create a feature class of the orders layer
    arcpy.conversion.ExportFeatures(active_orders_ufp, rivedo_feature_class)

    # Add columns to the temp feature layer
    add_columns_to_layer(rivedo_feature_class, config["query_inputs"], config["new_column_input"])

    
    shape_output(temp_output, temp_loc, config["excluded_priorities"], config["new_existing_field_mapping"])
