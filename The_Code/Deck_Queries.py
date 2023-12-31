# Author: Casey Betts, 2023
# This file contains the queries used to interrogate the tasking deck and output information on misprioritized orders

import arcpy
import pandas as pd
import json

from datetime import datetime
from math import floor 
from pathlib import Path

class Queries():
    """ Contains the qureie and output functions needed for the deck audit """

    def __init__(self, active_orders_ufp, hotlist_orders, path, username) -> None:
        """ Creates dataframe and sets varables """

        # Define the path to the parameters and the output
        self.parameters_path = Path( path + r"\The_Code\Sensitive_Parameters.json")
        self.output_path = path + r"\Results"

        # Load .json file with parameters
        with open(self.parameters_path, 'r') as input:
            parameters = json.load(input)

        # define parameter variables
        self.username = username
        self.new_pri_field_name = "Suggested_Priority"
        self.display_columns = parameters["columns_to_display"] + [self.new_pri_field_name]
        self.columns_to_drop = parameters["without_shapefile"]["columns_to_drop"]
        self.query_input = parameters["query_inputs"]
        self.arc_map_name = parameters["arc_map_name"]
        self.excluded_priorities = parameters["excluded_priorities"]
        self.customer_info = parameters["customer_info"]
        self.idi_cust_ids = list(self.customer_info["idi_customers"].keys())
        self.internal_cust_ids = list(self.customer_info["internal_customers"].keys())
        self.descriptions = list(self.query_input["project_descriptions"].keys())
        self.purchase_orders = list(self.query_input["project_purchase_orders"].keys())

        # Create empty dataframe to contain all results
        self.resulting_dataframe = pd.DataFrame()

        # Initialize and clean the active orders dataframe
        self.active_orders = active_orders_ufp
        self.clean_dataframe()

        # Get a list of SOLIs from the hotlist dataframe
        self.hotlist_SOLIs = hotlist_orders.soli.tolist()
        self.populate_new_priority()
        self.populate_customer_name()
        self.ending_digit_dataframe = self.ending_digit_query()
        self.output()

    def clean_dataframe(self):
        """ Removes unnecessary fields from a given active_orders_ufp dataframe """

        # Reindex the dataframe
        self.active_orders = self.active_orders.reset_index(drop=True)

        # Remove unnecessary columns
        self.active_orders.drop(labels=self.columns_to_drop, axis=1, inplace=True)

        # Change the tasking priority column to integer type
        self.active_orders = self.active_orders.astype({"tasking_priority": int}, copy=False)

        # Add column for customer name and the new priority
        self.active_orders[self.new_pri_field_name] = 0
        self.active_orders["Customer_Name"] = "--"

        # Remove unwanted tasking priorities
        self.active_orders = self.active_orders[~self.active_orders.tasking_priority.isin(self.excluded_priorities)]

        
        # Remove unwanted tasking priority and customer combinations
        for cust in self.query_input["customer_pri_combo_to_ignore"]:
            
            for pri in self.query_input["customer_pri_combo_to_ignore"][cust]:

                indexes_to_drop = self.active_orders[(self.active_orders.sap_customer_identifier == cust) & (self.active_orders.tasking_priority == pri)].index
                self.active_orders.drop(indexes_to_drop, inplace=True)

    def populate_customer_name(self):
        """ Populates the Customer_Name field with the name associated with the customer number if it exists """

        self.active_orders["Customer_Name"] = self.active_orders.apply(lambda x: self.customer_name(x.sap_customer_identifier), axis=1)

    def customer_name(self, cust):
        """ Returns the customer name given the customer number """ 

        if cust in self.idi_cust_ids:
            return self.customer_info["idi_customers"][cust]
        elif cust in self.internal_cust_ids:
            return self.customer_info["internal_customers"][cust]
        else:
            return "--"

    def populate_new_priority(self):
        """ Populates the given row with a new priority with the correct ending digit (to be used in the apply function for a given query) """

        # Populate order priorities based on customer or spacecraft
        self.active_orders[self.new_pri_field_name] = self.active_orders.apply(lambda x: self.correct_priority(x.tasking_priority, x.sap_customer_identifier, x.ge01, x.wv02, x.wv01), axis=1)

        # Populate order priorities base on project
        self.active_orders[self.new_pri_field_name] = self.active_orders.apply(lambda x: self.description_priority(x[self.new_pri_field_name], x.order_description), axis=1)
        self.active_orders[self.new_pri_field_name] = self.active_orders.apply(lambda x: self.po_priority(x[self.new_pri_field_name], x.purchase_order_header), axis=1)

    def description_priority(self, current_pri, description):
        """ Returns a priority according to the order's description """

        if description in self.descriptions:

            return self.query_input["project_descriptions"][description]
        
        else:
            return current_pri
        
    def po_priority(self, current_pri, purchase_order):
        """ Returns a priority according to the order's purchase_order """

        if purchase_order in self.purchase_orders:

            return self.query_input["project_purchase_orders"][purchase_order]
        
        else:
            return current_pri
    
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

    def high_pri_query(self, responsiveness):
        """ Returns a dataframe of orders of the given responsiveness that are below the appropriate priority """

        # Defines a slice of the active orders that are:
            #  1) the given responsivnes
            #  2) at a priority lower than the threshold and
            #  3) not in the excluded orders list
            #  4) not in the IDI customer list
        high_pri_orders = self.active_orders[
                            (self.active_orders.responsiveness_level == responsiveness) & 
                            (self.active_orders.tasking_priority < self.query_input["orders_at_high_pri"][responsiveness]["pri"]) & 
                            (~self.active_orders.sap_customer_identifier.isin(self.query_input["orders_at_high_pri"][responsiveness]["excluded_cust"]))
                            ].sort_values(by="sap_customer_identifier")
        
        # Drop any hotlist or IDI orders
        high_pri_orders = high_pri_orders[~high_pri_orders.external_id.isin(self.hotlist_SOLIs) &
                                              (~self.active_orders.sap_customer_identifier.isin(self.query_input["ending_digit_cust_list"]["1"]))]
        
        # Drop any orders prioritized based on PO or order description
        high_pri_orders = high_pri_orders[~high_pri_orders.purchase_order_header.isin(self.purchase_orders)]
        high_pri_orders = high_pri_orders[~high_pri_orders.order_description.isin(self.descriptions)]

        return high_pri_orders

    def low_pri_query(self, responsiveness):
        """ Returns a dataframe of orders of the given responsiveness that are above the appropriate priority """

        # Returns a slice of the active orders that are:
            #  1) the given responsiveness
            #  2) at a priority higher than the threshold and
            #  3) not in the excluded orders list
        low_pri_orders = self.active_orders[
                            (self.active_orders.responsiveness_level == responsiveness) & 
                            (self.active_orders.tasking_priority > self.query_input["orders_at_low_pri"][responsiveness]["pri"]) & 
                            (~self.active_orders.sap_customer_identifier.isin(self.query_input["orders_at_low_pri"][responsiveness]["excluded_cust"]))].sort_values(by="sap_customer_identifier")

        # Remove any orders that are specifically set to a middle digit by customer
        for middle_digit in self.query_input["middle_digit_cust_list"]:
                
                pri_list = [(700 + 10 * int(middle_digit) + x) for x in [1,2,3,4,5,6,7,8,9]]

                low_pri_orders = low_pri_orders[
                                    ~(low_pri_orders.sap_customer_identifier.isin(self.query_input["middle_digit_cust_list"][middle_digit]) & 
                                        low_pri_orders.tasking_priority.isin(pri_list) )]
                
        # Drop any orders prioritized based on PO or order description
        low_pri_orders = low_pri_orders[~low_pri_orders.purchase_order_header.isin(self.purchase_orders)]
        low_pri_orders = low_pri_orders[~low_pri_orders.order_description.isin(self.descriptions)]
                
        return low_pri_orders

    def ending_digit_query(self):
        """ For the given digit this will find all orders that do not have that digit and populate the new_pri column with the suggested priority """

        # Define dataframe that has a different suggested ending digit than the actual ending digit
        ending_digit_df = self.active_orders[(self.active_orders.tasking_priority) != (self.active_orders[self.new_pri_field_name])]

        # Return dataframe without any SOOPremium responsiveness
        return ending_digit_df[ending_digit_df.responsiveness_level != 'SOOPremium']
    
    def high_low_queries_string(self, query, responsiveness):
        """ Runs either a 'too high' or 'too low' query for the given responsiveness and returns a string of the results """ 

        output_string = ""

        # Define which query to use
        if query == "high": 
            func = self.high_pri_query
            output_string = "Hotlist and IDI orders are excluded from the \"high pri\" results\n\n"

        else: func = self.low_pri_query
        
        # Save the query results (dataframe) in a varable
        query_df = func(responsiveness)

        # Change the display term for 'None' responseveness to 'Spec'
        if responsiveness == "None": responsiveness_text = "Spec"
        else: responsiveness_text = responsiveness

        if query_df.empty:
            # Create a string stating that there were no orders found for this query
            output_string += "No " + responsiveness_text + " orders are prioritized " + query + "er than " + str(self.query_input["orders_at_"+query+"_pri"][responsiveness]["pri"])
        else:
            # Create a string of the appropriate heading as well as the dataframe with only the desired columns listed (suggested priority is omited)
            output_string += "These " + responsiveness_text + " orders are prioritized " + query + "er than " + str(self.query_input["orders_at_"+query+"_pri"][responsiveness]["pri"]) + ":\n" + query_df.loc[:, self.display_columns[:-1]].to_string()

        return output_string

    def ending_digit_query_string(self, digit, type):
        """ Runs all queries for orders with the wrong ending digit and returns a string of the results """ 

        output_string = ""

        if type == "has":
            # Find the slice of the dataframe where the CURRENT priority ends in the given digit
            result = self.ending_digit_dataframe[self.ending_digit_dataframe.tasking_priority % 10 == digit].sort_values(by="sap_customer_identifier")
            
            # If the dataframe is not empty then display it for orders that SHOULD have the given digit
            if result.empty:
                output_string += "No orders found with an erroneous ending digit of " + str(digit)
            else:
                output_string += "These orders should not have an ending digit of " + str(digit) + "\n"
                output_string += result.loc[:, self.display_columns].to_string()

        elif type == "has_not":
            # Find the slice of the dataframe where the SUGGESTED priority ends in the given digit
            result = self.ending_digit_dataframe[self.ending_digit_dataframe[self.new_pri_field_name] % 10 == digit].sort_values(by="sap_customer_identifier")

            # If the dataframe is not empty then display it for orders that SHOULD NOT have the given digit
            if result.empty:
                output_string += "No orders need to be changed to have an ending digit of " + str(digit)
            else:
                output_string += "These orders should have an ending digit of " + str(digit) + "\n"
                output_string += result.loc[:, self.display_columns].to_string()

        return output_string
                            
    def output(self):
        """ Creates a text file with the desired info """

        output_string = ""

        # Appends ending digit text to string for each ending digit
        # for digit in range(1,10):
        #     for type in ["has", "has_not"]:
        #         output_string +=  self.ending_digit_query_string(digit, type)
        #         output_string += "\n\n\n"

        # Appends middle digit text to string for each query criteria
        for query in ["high", "low"]:
            for responsiveness in ['None', 'Select', 'SelectPlus', 'SOOPremium']:
                output_string += self.high_low_queries_string(query, responsiveness)
                output_string += "\n\n\n"

        # Create a timestamp string
        timestamp = str(datetime.now())[:19]
        timestamp = timestamp.replace(':','-')

        # Creates output file with above strings as text
        with open(self.output_path + "\\" + self.username + " " + timestamp + " Text.txt", 'w') as f:
            f.write(output_string)

        # Creates a .csv file from the dataframe of all changes needed
        self.ending_digit_dataframe.loc[:, self.display_columns].sort_values(by="sap_customer_identifier").to_csv(self.output_path + "\\" + self.username + " " + timestamp + " Table.csv")

