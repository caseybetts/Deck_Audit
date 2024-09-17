# Author: Casey Betts, 2023
# This file contains the queries used to interrogate the tasking deck and output information on misprioritized orders

import arcpy
import os
import json
import shutil

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

def add_columns_to_feature_class(feature_class, query_input, new_column_input):
    """
    Adds the given columns to the given feature layer

    :param feature_class: Feature Class, the feature class to have columns added
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
        arcpy.management.CalculateField(feature_class, field_name, expression, "PYTHON3", code_block, field_type)

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

def select_and_delete_rows(layer, rivedo_where_clause, excluded_priorities):
    """ 
    Selects and deletes rows based on a given set of criteria
    
    :param layer: Feature Layer, the layer to be edited
    :param excluded_priorities, List, a list of priorities to exclude
    """

    # Select the desired rows
    arcpy.management.SelectLayerByAttribute(layer, "NEW_SELECTION", rivedo_where_clause)
    arcpy.management.SelectLayerByAttribute(layer, "SWITCH_SELECTION")
    exclusion_string = "(" + ",".join(str(num) for num in excluded_priorities) + ")"
    exlusion_where_clause = "tasking_priority"[:10] + " IN " + exclusion_string
    arcpy.management.SelectLayerByAttribute(layer, "ADD_TO_SELECTION", exlusion_where_clause)
    arcpy.management.DeleteFeatures(layer)

def delete_current_files(file_location, file_names):
    """ Deletes out the current files in the folder """

    for file in file_names:
        file_path = os.path.join(file_location,  file)
        if os.path.exists(file_path):
            os.remove(file_path)
            arcpy.AddMessage(f"Deleted: {file}")

def move_new_files(staging_location, output_location):
    """ Moves the newly generated files to the output folder """

    for file in os.listdir(staging_location):
        source_path = os.path.join(staging_location, file)
        dest_path = os.path.join(output_location, file)
        shutil.move(source_path, dest_path)
        arcpy.AddMessage(f"Moved: {file}")

def run(active_orders_ufp, path):
    """ This is the function called from the arc tool """

    # Define the paths to the parameters and the outputs
    parameters_path = Path( path + r"\The_Code\Sensitive_Parameters.json")
    temp_loc = arcpy.env.workspace
    temp_name = "Rivedo_temp"
    temp_feature_class = temp_loc + "\\" + temp_name
    output_loc = path + r"\Shapefile"
    output_name = "Rivedo_orders"
    staging_location = os.path.join(path, "Shapefile_Staging")

    # Load .json file with parameters
    with open(parameters_path, 'r', errors="ignore") as input:
        config = json.load(input)

    # Generate the new field mapping
    new_field_mapping = produce_field_mapping(temp_name, len(config["new_column_input"]), config["new_existing_field_mapping"])

    # Set the string for the where clause that determines which rows to remove   
    rivedo_priority = config["new_column_input"]["Rivedo_Priority"]["field_name"]
    middle_digit = config["new_column_input"]["Middle_Digit"]["field_name"]
    rivedo_where_clause = "tasking_priority"[:10] + f"<> {rivedo_priority} Or {middle_digit} = 'Low' Or {middle_digit} = 'High' "

    # Get the active map document and data frame
    project = arcpy.mp.ArcGISProject("CURRENT")
    map = project.activeMap

    # Create a temporary feature class of the orders layer
    arcpy.conversion.ExportFeatures(active_orders_ufp, temp_feature_class)

    # Add columns to the temp feature class
    add_columns_to_feature_class(temp_feature_class, config["query_inputs"], config["new_column_input"])

    # Add the feature layer to the map
    map.addDataFromPath(temp_feature_class)

    # Create a new feature class with the reordered fields
    arcpy.conversion.ExportFeatures(temp_name, staging_location + "\\" + output_name, field_mapping = new_field_mapping)

    # Delete the existing output files
    delete_current_files(output_loc, output_name)

    # Move the produced files to the final output location
    move_new_files(staging_location, output_loc)

    # Remove the temp layer from the map
    map.removeLayer(get_layer_by_name(temp_name, map))

    select_and_delete_rows(get_layer_by_name(output_name, map), rivedo_where_clause, config["excluded_priorities"])
