# Author: Casey Betts, 2023
# This file contains the queries used to interrogate the tasking deck and output information on misprioritized orders

import arcpy
import os
import json
import shutil

from datetime import datetime
from helper_functions import *
from math import floor 
from pathlib import Path


class Rivedo():
    """ Object used to produce the Rivedo shapefile """

    def __init__(self, active_orders_ufp, hotlist, path, username):
        """ Load and initiate data and vars 
        
            :param active_orders_ufp: Feature Layer, active orders ufp layer
            :param hotlist: Feature Layer, the hotlist layer
            :param path: String, path to the folder where the config and output folder is kept
        """

        self.config_path = os.path.join(path + "\\The_Code")
        config_name = "Sensitive_Parameters.json"


        # Load .json config file
        with open(os.path.join(self.config_path, config_name), 'r', errors="ignore") as input:
            self.config = json.load(input)

        # Define the paths to the parameters and the outputs
        self.temp_loc = arcpy.env.workspace
        self.temp_name = "Rivedo_temp"
        self.temp_feature_class = self.temp_loc + "\\" + self.temp_name
        self.output_loc = path + r"\Shapefile"
        self.output_name = "Rivedo_orders"
        self.staging_location = os.path.join(path, "Shapefile_Staging")
        self.active_orders_ufp = active_orders_ufp
        self.hotlist = hotlist
        self.row_count = 0
        self.username = username
        self.metrics = dict()

        # Produce active customer dict
        self.active_cust_info = self.produce_cust_info()

        # Get the active map document and data frame
        self.project = arcpy.mp.ArcGISProject("CURRENT")
        self.map = self.project.activeMap

        # Create hotlist soli list and where clause string
        hotlist_solis = self.get_field_values(self.hotlist, "soli")

        # Create vars for the rivedo priority and middle digit from the configs 
        rivedo_priority = self.config["new_column_input"]["Rivedo_Priority"]["field_name"]
        middle_digit = self.config["new_column_input"]["Middle_Digit"]["field_name"]


        # Create a list of where clauses to select rows to delete
        self.delete_clauses = ["tasking_priority" + f" = {rivedo_priority} And {middle_digit} <> 'Low' And {middle_digit} <> 'High' ", 
                              "tasking_priority" + " IN " + "(" + ",".join(str(num) for num in self.config["excluded_priorities"]) + ")", 
                              "ge01 = 0 And wv01 = 0 And wv02 = 0 And wv03 = 0",
                              "sap_customer_identifier" + " IN " + "(" + ",".join("'"+str(num)+"'" for num in self.config["customer_info"]["idi_customers"]) + ")", 
                              "external_id" + " IN " + "(" + ",".join("'"+str(num)+"'" for num in hotlist_solis) + ")"]

        # Call functions
        self.run_workflow()


    def add_columns_to_feature_class(self, active_customer_info):
        """
        Adds the given columns to the given feature layer

        """

        for new_column in self.config["new_column_input"]:
            
            # Create column specific vars
            field_name = self.config["new_column_input"][new_column]["field_name"]
            expression = self.config["new_column_input"][new_column]["expression"]
            config_reqs = self.config["new_column_input"][new_column]["config_reqs"]
            field_type = self.config["new_column_input"][new_column]["field_type"]

            # Create code block string
            with open(os.path.join(self.config_path, self.config["new_column_input"][new_column]['column_function']), 'r') as data:
                column_function = data.read()
            
            if config_reqs == "active_customer_info":
                code_block = "active_customer_info = " + str(active_customer_info) + "\n"
            else:
                code_block = config_reqs + " = " + str(self.config[config_reqs]) + "\n"

            code_block = code_block + column_function

            # Add column to feature class
            arcpy.management.CalculateField(self.temp_feature_class, field_name, expression, "PYTHON3", code_block, field_type)

    def produce_field_mapping(self):
        """
        Creates a field mapping as input for creating a new feature class

        :param input_layer: String, the name of the input feature class or table
        :param fields_to_move: Integer, the number of fields to move from the end to the beginning
        :return: String, the name of the new feature class or table with reordered fields
        """
        arcpy.env.overwriteOutput = True

        # Get a list of all field names minus the existing ones that will be moved
        fields = arcpy.ListFields(self.temp_name)
        field_names = [field.name for field in fields if field.type not in ['OID', 'Geometry'] + self.config["new_mapping_for_existing_fields"]]

        # Determine the new field order
        number_of_fields_to_move = min(len(self.config["new_column_input"]), len(field_names))  # Ensure we don't try to move more fields than exist
        new_field_order = self.config["new_mapping_for_existing_fields"] + field_names[-number_of_fields_to_move:] + field_names[:-number_of_fields_to_move]

        # Create a FieldMappings object
        field_mappings = arcpy.FieldMappings()
        field_mappings.addTable(self.temp_name)

        # Create a new FieldMappings object with fields in the desired order
        new_field_mappings = arcpy.FieldMappings()
        for field_name in new_field_order:
            field_index = field_mappings.findFieldMapIndex(field_name)
            if field_index != -1:
                field_map = field_mappings.getFieldMap(field_index)
                new_field_mappings.addFieldMap(field_map)

        return new_field_mappings
    
    def customer_name(self, cust, customer_info):
        """ 
        Returns the customer name given the customer number and the dict of all customers
        
        :param cust: Str, customer id
        :param customer_info: Dict, dictionary of dictionaries of customer ids and names
        """

        for item in customer_info:
            if cust in customer_info[item]:
                return customer_info[item][cust]
        
        return "--"
    
    def get_layer_by_name(self, layer_name, map):
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
        
    def delete_current_files(self):
        """ 
        Deletes out the files of the given name in the given folder
        
        :param file_location: String, path to the target folder
        :param file_names: String, name of file(s) to be deleted
        """

        for file in self.output_name:
            file_path = os.path.join(self.output_loc,  file)
            if os.path.exists(file_path):
                os.remove(file_path)
                arcpy.AddMessage(f"Deleted: {file}")

    def move_files(self):
        """ 
        Moves all files in a given folder to the given output folder
        
        :param initial_location: String, path to the 'move from' folder
        :param final_location: String, path to the 'move to' folder
        """

        for file in os.listdir(self.staging_location):
            source_path = os.path.join(self.staging_location, file)
            dest_path = os.path.join(self.output_loc, file)
            shutil.move(source_path, dest_path)
         
    def produce_cust_info(self):
        """
        Returns a dictionary of only active customer ids and names

        :param active_orders: Feature Layer, active orders ufp layer
        :param customer_info: Dict, all customer info from the config file
        """

        # Create a set of unique customer IDs
        with arcpy.da.SearchCursor(self.active_orders_ufp, ["sap_customer_identifier"]) as cursor:
            active_customers = sorted({row[0] for row in cursor})

        customer_dict = dict()
        
        # For each type of customer filter the dictionary elements down to only those cooresponding with the active customers list
        # Then combine to to the existing dictionary
        for customers in self.config["customer_info"]:
            active_customer_dict = {id:self.config["customer_info"][customers][id] for id in active_customers if id in self.config["customer_info"][customers]}
            customer_dict.update(active_customer_dict)

        return customer_dict

    def get_field_values(self, layer, field):
        """ 
        Returns a list of values from a given field in a given layer
        
        :param layer: Feature Layer, the layer with the values to stor in a list
        :param field: String, the name of the field to gather values from
        """

        # initialize list
        values = []

        # Append all values into the list
        with arcpy.da.SearchCursor(layer, [field]) as cursor:
            for row in cursor:
                values.append(row[0])

        return values

    def get_metrics(self, layer):
        """
        Find and store metrics in the metric dictionary
        """

        for metric in self.config["metrics"]:
            query = self.config["metrics"][metric][0]
            if query:
                arcpy.management.SelectLayerByAttribute(layer, "NEW_SELECTION", query)
                self.config["metrics"][metric][1] = arcpy.management.GetCount(layer)

    def display_metrics(self):
        """ 
        Output metrics info to the messaging 
        """

        # Display number of unique customers
        arcpy.AddMessage("Number of Unique Customers: " + str(len(self.active_cust_info)))

        # Display metrics specified from config file
        for metric in self.config["metrics"]:
            arcpy.AddMessage(metric + " : " + str(self.config["metrics"][metric][1]))

    def select_and_delete_rows(self, layer):
        """ Select and delete rows from the given layer given a list of where clauses

        :param layer, Feature Layer, the layer to select and delete rows from
        """

        # Select rows to delete
        for clause in self.delete_clauses:
            arcpy.management.SelectLayerByAttribute(layer, "ADD_TO_SELECTION", clause)

        # Delete selected rows
        arcpy.management.DeleteFeatures(layer)

        # Get number of rows in the layer file
        self.row_count = arcpy.management.GetCount(layer)
        arcpy.AddMessage("Records: " + str(self.row_count))

    def update_log(self):
        """
        Updates a text file with details on the run
        """
        # Create a timestamp string
        timestamp = str(datetime.now())[:19]
        timestamp = timestamp.replace(':','-')

        # Open text file
        with open("Rivedo_Log.txt", "a") as log:
            
            # Update text file
            log.write("\n" + self.username + "     " + timestamp + "     ")
            log.write("Records returned: " + str(self.row_count))

    def run_workflow(self):
        """ This function calls all functions in the needed order to produce final output """

        # Create a temporary feature class of the orders layer
        arcpy.conversion.ExportFeatures(self.active_orders_ufp, self.temp_feature_class)

        # Add columns to the temp feature class
        self.add_columns_to_feature_class(self.active_cust_info)

        # Generate the new field mapping
        new_field_mapping = self.produce_field_mapping()

        # Add the feature calss to the map as a new feature layer
        self.map.addDataFromPath(self.temp_feature_class)

        # Select and Delete rows from the temp featuer layer
        self.select_and_delete_rows(self.temp_name) 

        # Create a new feature class with the reordered fields
        arcpy.conversion.ExportFeatures(self.temp_name, self.staging_location + "\\" + self.output_name, field_mapping = new_field_mapping)

        # Delete the existing output files
        self.delete_current_files()

        # Move the produced files to the final output location
        self.move_files()

        # Get metrics
        self.get_metrics(self.get_layer_by_name(self.temp_name, self.map))

        # Display metrics
        self.display_metrics()

        # Remove the temp layer from the map
        self.map.removeLayer(self.get_layer_by_name(self.temp_name, self.map))

        # Log the run
        self.update_log()


