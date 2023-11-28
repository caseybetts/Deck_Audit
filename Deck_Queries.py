# This file contains the queries used to interrogate the tasking deck and will return the results

import arcpy
import pandas as pd
import json
from math import floor 
from pathlib import Path

path = r"C:\Users\ca003927\Music\Git\Deck_Audit"

# Paths to the active orders UFP, parameters and output
parameters_path = Path( path + r"\Local_only\Sensitive_Parameters.json")
output_path = Path( path + r"\Local_only\output.txt")

with open(parameters_path, 'r') as input:
    parameters = json.load(input)


class Queries():
    """ Contains the qureie and output functions needed for the deck audit """

    def __init__(self, active_orders_ufp) -> None:
        """ Creates dataframe and sets varables """

        # define parameter variables
        self.new_pri_field_name = "Suggested_Priority"
        self.display_columns = parameters["columns_to_display"] + [self.new_pri_field_name]
        self.columns_to_drop = parameters["without_shapefile"]["columns_to_drop"]
        self.query_input = parameters["query_inputs"]
        self.arc_project_loc = parameters["arc_project_path"]
        self.arc_map_name = parameters["arc_map_name"]
        self.excluded_priorities = parameters["excluded_priorities"]

        # Create empty dataframe to contain all results
        self.resulting_dataframe = pd.DataFrame()

        # Create and clean the dataframe
        self.active_orders = self.create_dataframe()
        self.clean_dataframe()
        self.populate_new_priority()
        self.output()

    def create_dataframe(self):
        """ Searches the map contents for the active orders layer and returns a dataframe from it """
        # Set variables to current project and map
        aprx = arcpy.mp.ArcGISProject("current")
        map = aprx.activeMap

        # Search layers for the active orders
        for layer in map.listLayers():
            if layer.isFeatureLayer:
                if layer.name == 'PROD_Active_Orders_UFP':
                    break

        # Read the geo database table into pandas dataframe
        fields = [f.name for f in arcpy.ListFields(layer)]

        with arcpy.da.SearchCursor(layer, fields) as cursor:
            df = pd.DataFrame(list(cursor), columns=fields)

        return df

    def clean_dataframe(self):
        """ Removes unnecessary fields from a given active_orders_ufp dataframe """

        # Remove unnecessary columns
        self.active_orders.drop(labels=self.columns_to_drop, axis=1, inplace=True)

        # Add column for the new priority
        self.active_orders[self.new_pri_field_name] = 0

        # Remove unwanted tasking priorities
        self.active_orders = self.active_orders[~self.active_orders.tasking_priority.isin(self.excluded_priorities)]

    def high_pri_query(self, responsiveness):
        """ Returns a dataframe of orders of the given responsiveness that are below the appropriate priority """

        return self.active_orders[
                        (self.active_orders.responsiveness_level == responsiveness) & 
                        (self.active_orders.tasking_priority < self.query_input["orders_at_high_pri"][responsiveness]["pri"]) & 
                        (~self.active_orders.sap_customer_identifier.isin(self.query_input["orders_at_high_pri"][responsiveness]["excluded_cust"]))].sort_values(by="sap_customer_identifier")
    
    def low_pri_query(self, responsiveness):
        """ Returns a dataframe of orders of the given responsiveness that are above the appropriate priority """

        return self.active_orders[
                        (self.active_orders.responsiveness_level == responsiveness) & 
                        (self.active_orders.tasking_priority > self.query_input["orders_at_low_pri"][responsiveness]["pri"]) & 
                        (~self.active_orders.sap_customer_identifier.isin(self.query_input["orders_at_low_pri"][responsiveness]["excluded_cust"]))].sort_values(by="sap_customer_identifier")

    def ending_digit_query(self):
        """ For the given digit this will find all orders that do not have that digit and populate the new_pri column with the suggested priority """

        return self.active_orders[(self.active_orders.tasking_priority % 10) != (self.active_orders[self.new_pri_field_name] % 10)]
    
    def populate_new_priority(self):
        """ Populates the given row with a new priority with the correct ending digit (to be used in the apply function for a given query) """

        # Populate orders that have a customer based criteria
        self.active_orders[self.new_pri_field_name] = self.active_orders.apply(lambda x: self.correct_priority(x.tasking_priority, x.sap_customer_identifier, x.ge01, x.wv02, x.wv01), axis=1)
    
    def correct_priority(self, priority, cust, ge01, wv02, wv01):
        """ Returns a priority according a 'discision tree' for the given order parameters """

        # Sets the middle digit
        if cust in self.query_input["middle_digit_cust_list"]["1"]:
            middle_digit = 1
        elif cust in self.query_input["middle_digit_cust_list"]["2"]:
            middle_digit = 2
        elif cust in self.query_input["middle_digit_cust_list"]["3"]:
            middle_digit = 3
        elif cust in self.query_input["middle_digit_cust_list"]["4"]:
            middle_digit = 4
        elif cust in self.query_input["middle_digit_cust_list"]["5"]:
            middle_digit = 5
        elif cust in self.query_input["middle_digit_cust_list"]["6"]:
            middle_digit = 6
        elif cust in self.query_input["middle_digit_cust_list"]["7"]:
            middle_digit = 7
        elif cust in self.query_input["middle_digit_cust_list"]["8"]:
            middle_digit = 8
        elif cust in self.query_input["middle_digit_cust_list"]["9"]:
            middle_digit = 9
        elif cust in self.query_input["middle_digit_cust_list"]["0"]:
            middle_digit = 0
        else:
            middle_digit = floor((priority - 700)/10)

        # Sets the ending digit
        if cust in self.query_input["ending_digit_cust_list"]["1"]:
            ending_digit = 1
        elif cust in self.query_input["ending_digit_cust_list"]["2"]:
            ending_digit = 2
        elif cust in self.query_input["ending_digit_cust_list"]["6"]:
            ending_digit = 6
        elif cust in self.query_input["ending_digit_cust_list"]["7"]:
            ending_digit = 7
        elif cust in self.query_input["ending_digit_cust_list"]["8"]:
            ending_digit = 8
        elif cust in self.query_input["ending_digit_cust_list"]["9"]:
            ending_digit = 9
        elif cust in self.query_input["ending_digit_cust_list"]["0"]:
            ending_digit = 0
        elif (ge01 == 0) and (wv02 ==0) and (wv01 == 0):
            ending_digit = 3
        else:
            ending_digit = 4

        return 700 + (middle_digit * 10) + ending_digit

    def high_low_queries_string(self, query, responsiveness):
        """ Runs all queries for orders prioritized too high or too low and returns a string of the results """ 

        output_string = ""

        # Run all queries for the middle digit (prioritized too high or too low)

        if query == "high": func = self.high_pri_query
        else: func = self.low_pri_query
        
        query_df = func(responsiveness)

        if query_df.empty:
            output_string += "No " + responsiveness + " orders seemed to be too " + query
        else:
            output_string += "These " + responsiveness + " orders may be too " + query + "\n" + query_df.loc[:, self.display_columns[:-1]].to_string()

        return output_string

    def ending_digit_querie_string(self, digit, type):
        """ Runs all queries for orders with the wrong ending digit and returns a string of the results """ 

        output_string = ""

        # Find the slice of the dataframe where the current priority and correct priority are different
        if type == "has":
            result = self.active_orders[(self.active_orders[self.new_pri_field_name] % 10 == digit) & (self.active_orders.tasking_priority % 10 != digit)].sort_values(by="sap_customer_identifier")

            # If the dataframe is not empty display it for orders that should have the given digit
            if result.empty:
                output_string += "No orders need to be changed to have an ending digit of " + str(digit)
            else:
                output_string += "These orders should have an ending digit of " + str(digit) + "\n"
                output_string += result.loc[:, self.display_columns].to_string()

        elif type == "has_not":
            result = self.active_orders[(self.active_orders[self.new_pri_field_name] % 10 != digit) & (self.active_orders.tasking_priority % 10 == digit)].sort_values(by="sap_customer_identifier")

            # If the dataframe is not empty display it for orders that should not have the given digit
            if result.empty:
                output_string += "No orders found with an erroneous ending digit of " + str(digit)
            else:
                output_string += "These orders should not have an ending digit of " + str(digit) + "\n"
                output_string += result.loc[:, self.display_columns].to_string()

        return output_string
                            
    def output(self):
        """ Creates a text file with the desired info """

        output_string = ""

        # Appends ending digit text to string for each ending digit
        for digit in range(1,10):
            for type in ["has", "has_not"]:
                output_string +=  self.ending_digit_querie_string(digit, type)
                output_string += "\n\n\n"

        # Appends middle digit text to string for each query criteria
        for query in ["high", "low"]:
            for responsiveness in ['None', 'Select', 'SelectPlus']:
                output_string += self.high_low_queries_string(query, responsiveness)
                output_string += "\n\n\n"

        # Creates output file with above strings as text
        with open(path + r"\Local_only\output.txt", 'w') as f:
            f.write(output_string)

        # Creates a .csv file from the dataframe of all changes needed
        self.ending_digit_query().loc[:, self.display_columns].to_csv(path + r"\Local_only\changes_needed.csv")

