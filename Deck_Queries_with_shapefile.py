# This file contains the queries used to interrogate the tasking deck and will return the results
# Requires shapefile (.dbf)

import geopandas as gpd
import pandas as pd
import json
from math import floor 
from pathlib import Path
from sys import argv

given_path = argv[1]


# Paths to the active orders UFP, parameters and output
# active_orders_path = Path(r"C:\Users\cr003927\OneDrive - Maxar Technologies Holdings Inc\Private Drop\Git\Deck_Audit\Local_only\PROD_Active_Orders_UFP_pri690-800.shp")
# parameters_path = Path(r"C:\Users\cr003927\OneDrive - Maxar Technologies Holdings Inc\Private Drop\Git\Deck_Audit\Local_only\Sensitive_Parameters.json")
# output_path = Path(r"C:\Users\cr003927\OneDrive - Maxar Technologies Holdings Inc\Private Drop\Git\Deck_Audit\Local_only\output.txt")
# pickle_path = Path(r"C:\Users\cr003927\OneDrive - Maxar Technologies Holdings Inc\Private Drop\Git\Deck_Audit\Local_only\orders_dataframe.pkl")

active_orders_path = Path(given_path + r"\Local_only\PROD_Active_Orders_UFP_Nov20.shp")
parameters_path = Path(given_path + r"\Local_only\Sensitive_Parameters.json")
output_path = Path(given_path + r"\Local_only\output.txt")
pickle_path = Path(given_path + r"\Local_only\orders_dataframe.pkl")

with open(parameters_path, 'r') as input:
    parameters = json.load(input)


class Queries():
    """ Contains the qureie and output functions needed for the deck audit """

    def __init__(self) -> None:
        """ Creates dataframe and sets varables """

        # define parameter variables
        self.new_pri_field_name = "New_Pri"
        self.display_columns = parameters["columns_to_display"] + [self.new_pri_field_name]
        self.columns_to_drop = parameters["with_shapefile"]["columns_to_drop"]
        self.query_input = parameters["query_inputs"]
        self.arc_project_loc = parameters["arc_project_path"]
        self.arc_map_name = parameters["arc_map_name"]
        self.excluded_priorities = parameters["excluded_priorities"]
        

        # Create empty dataframe to contain all results
        self.resulting_dataframe = pd.DataFrame()

        # Create and clean the dataframe
        self.active_orders = self.create_dataframe(active_orders_path)
        self.populate_new_priority()
        self.output()

    def create_dataframe(self, source_file_path):
        """ Reads the .pkl file into a pandas dataframe """ 

        if pickle_path.is_file():
            df = pd.read_pickle(pickle_path)
        else:
            # if the pickel file does not exist then create it from the active ordrs .dbf file
            df = pd.DataFrame(gpd.read_file(source_file_path))

            # Remove unnecessary columns
            df.drop(labels=self.columns_to_drop, axis=1, inplace=True)

            # Change truncated field names
            df.rename(columns={ "external_i": "external_id",
                                "tasking_pr": "tasking_priority", 
                                "responsive": "responsiveness_level",
                                "sap_custom":"sap_customer_identifier"}, inplace=True)
            
            # Remove unwanted tasking priorities
            df = df[~df.tasking_priority.isin(self.excluded_priorities)]

            # Add column for the new priority
            df[self.new_pri_field_name] = 0

            df.to_pickle(pickle_path)

        return df

    def high_pri_query(self, responsiveness):
        """ Identifies orders of the given responsiveness that are below the appropreate priority """

        return self.active_orders[
                        (self.active_orders.responsiveness_level == responsiveness) & 
                        (self.active_orders.tasking_priority < self.query_input["orders_at_high_pri"][responsiveness]["pri"]) & 
                        (~self.active_orders.sap_customer_identifier.isin(self.query_input["orders_at_high_pri"][responsiveness]["excluded_cust"]))]
    
    def low_pri_query(self, responsiveness):
        """ Identifies orders of the given responsiveness that are above the appropreate priority """

        return self.active_orders[
                        (self.active_orders.responsiveness_level == responsiveness) & 
                        (self.active_orders.tasking_priority > self.query_input["orders_at_low_pri"][responsiveness]["pri"]) & 
                        (~self.active_orders.sap_customer_identifier.isin(self.query_input["orders_at_low_pri"][responsiveness]["excluded_cust"]))]

    def ending_digit_query(self):
        """ For the given digit this will find all orders that do not have that digit and populate the new_pri column with the suggested priority """

        return self.active_orders[(self.active_orders.tasking_priority % 10) != (self.active_orders.New_Pri % 10)]

    def populate_new_priority(self):
        """ Populates the given row with a new priority with the correct ending digit (to be used in the apply function for a given query) """

        # Populate orders that have a customer based criteria
        self.active_orders.New_Pri = self.active_orders.apply(lambda x: self.correct_priority(x.tasking_priority, x.sap_customer_identifier, x.ge01, x.wv02, x.wv01), axis=1)
    
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
            result = self.active_orders[(self.active_orders.New_Pri % 10 == digit) & (self.active_orders.tasking_priority % 10 != digit)]

            # If the dataframe is not empty display it for orders that should have the given digit
            if result.empty:
                output_string += "No orders need to be changed to have an ending digit of " + str(digit)
            else:
                output_string += "These orders should have an ending digit of " + str(digit) + "\n"
                output_string += result.loc[:, self.display_columns].to_string()

        elif type == "has_not":
            result = self.active_orders[(self.active_orders.New_Pri % 10 != digit) & (self.active_orders.tasking_priority % 10 == digit)]

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

        # Appends middle digit text to string for each query criteria
        for query in ["high", "low"]:
            for responsiveness in ['None', 'Select', 'SelectPlus']:
                output_string += self.high_low_queries_string(query, responsiveness)
                output_string += "\n\n\n"

        # Appends ending digit text to string for each ending digit
        for digit in range(1,10):
            for type in ["has", "has_not"]:
                output_string +=  self.ending_digit_querie_string(digit, type)
                output_string += "\n\n\n"

        # Creates output file with above strings as text
        with open(given_path + r"\Local_only\output.txt", 'w') as f:
            f.write(output_string)

        # Creates a .csv file from the dataframe of all changes needed
        self.ending_digit_query().loc[:, self.display_columns].to_csv(given_path + r"\Local_only\changes_needed.csv")



queries = Queries()

