# This file contains the queries used to interrogate the tasking deck and will return the results
# Requires shapefile (.dbf)

import geopandas as gpd
import pandas as pd
import json
from math import floor 
from pathlib import Path

# Paths to the active orders UFP, parameters and output
active_orders_path = Path(r"C:\Users\cr003927\OneDrive - Maxar Technologies Holdings Inc\Private Drop\Git\Deck_Audit\Local_only\PROD_Active_Orders_UFP_pri690-800.shp")
parameters_path = Path(r"C:\Users\cr003927\OneDrive - Maxar Technologies Holdings Inc\Private Drop\Git\Deck_Audit\Local_only\Sensitive_Parameters.json")
output_path = Path(r"C:\Users\cr003927\OneDrive - Maxar Technologies Holdings Inc\Private Drop\Git\Deck_Audit\Local_only\output.txt")
pickle_path = Path(r"C:\Users\cr003927\OneDrive - Maxar Technologies Holdings Inc\Private Drop\Git\Deck_Audit\Local_only\orders_dataframe.pkl")

with open(parameters_path, 'r') as input:
    parameters = json.load(input)


class Queries():
    """ Contains the qureie and output functions needed for the deck audit """

    def __init__(self) -> None:
        """ Creates dataframe and sets varables """

        # define parameter variables
        self.display_columns = parameters["with_shapefile"]["columns to display"]
        self.columns_to_drop = parameters["with_shapefile"]["columns_to_drop"]
        self.query_input = parameters["query_inputs"]
        self.arc_project_loc = parameters["arc_project_path"]
        self.arc_map_name = parameters["arc_map_name"]
        self.idi_customers = parameters["IDI_customers"]

        # Create empty dataframe to contain all results
        self.resulting_dataframe = pd.DataFrame()

        # Create and clean the dataframe
        self.active_orders = self.create_dataframe(active_orders_path)
        self.clean_dataframe()
        self.populate_new_pri()

        
        output_string = self.ending_digit_query().loc[:, self.display_columns].to_string()

        # Creates output file with above strings as text
        with open(r"C:\Users\cr003927\OneDrive - Maxar Technologies Holdings Inc\Private Drop\Git\Deck_Audit\Local_only\output.txt", 'w') as f:
            f.write(output_string)
        

    def create_dataframe(self, source_file_path):
        """ Reads the .pkl file into a pandas dataframe """ 

        if pickle_path.is_file():
            df = pd.read_pickle(pickle_path)
        else:
            # if the pickel file does not exist then create it from the active ordrs .dbf file
            df = pd.DataFrame(gpd.read_file(source_file_path))
            df.to_pickle(pickle_path)

        return df

    def clean_dataframe(self):
        """ Removes unnecessary fields from a given active_orders_ufp dataframe """
        
        # Remove unnecessary columns
        # self.active_orders.drop(labels=self.columns_to_drop, axis=1, inplace=True)

        # Add column for the new priority
        self.active_orders["New_Pri"] = 0

        # Remove tasking priorities above 690
        self.active_orders = self.active_orders[(self.active_orders.tasking_pr > 690)]

    def orders_at_high_pri(self, responsiveness):
        """ Identifies orders of the given responsiveness that are below the appropreate priority """

        return self.active_orders[
                        (self.active_orders.responsive == responsiveness) & 
                        (self.active_orders.tasking_pr < self.query_input["orders_at_high_pri"][responsiveness]["pri"]) & 
                        (~self.active_orders.sap_custom.isin(self.query_input["orders_at_high_pri"][responsiveness]["excluded_cust"]))]
    
    def orders_at_low_pri(self, responsiveness):
        """ Identifies orders of the given responsiveness that are above the appropreate priority """

        return self.active_orders[
                        (self.active_orders.responsive == responsiveness) & 
                        (self.active_orders.tasking_pr > self.query_input["orders_at_low_pri"][responsiveness]["pri"]) & 
                        (~self.active_orders.sap_custom.isin(self.query_input["orders_at_low_pri"][responsiveness]["excluded_cust"]))]

    def ending_digit_query(self):
        """ For the given digit this will find all orders that do not have that digit and populate the new_pri column with the suggested priority """

        return self.active_orders[(self.active_orders.tasking_pr % 10) != (self.active_orders.New_Pri % 10)]

            
    def populate_new_pri(self):
        """ Populates the given row with a new priority with the correct ending digit (to be used in the apply function for a given query) """

        self.active_orders.New_Pri = self.active_orders.apply(lambda x: self.correct_priority(x.tasking_pr, x.sap_custom), axis=1)

    
    def correct_priority(self, priority, cust):

        if cust in self.query_input["ending_digit_cust_list"]["1"]:
            ending_digit = 1
        elif cust in self.query_input["ending_digit_cust_list"]["2"]:
            ending_digit = 2
        elif cust in self.query_input["ending_digit_cust_list"]["6"]:
            ending_digit = 6
        elif cust in self.query_input["ending_digit_cust_list"]["8"]:
            ending_digit = 8
        elif cust in self.query_input["ending_digit_cust_list"]["9"]:
            ending_digit = 9
        else:
            return 0

        return 700 + floor((priority - 700)/10) * 10 + ending_digit
    
                                   
    def output(self):
        """ Creates a text file with the desired info """     

        output_string = ""

        # Run all queries for the middle digit (prioritized too high or too low)
        # for query in ["high", "low"]:
        #     for responsiveness in ['None', 'Select', 'SelectPlus']:
        #         if query == "high": func = self.orders_at_high_pri
        #         else: func = self.orders_at_low_pri
                
        #         output_string += "\nThese " + responsiveness + " orders may be too " + query + func(responsiveness).loc[:, self.display_columns].to_string()


        # Find and append results of all the ending digit queries if they exist
        for digit in range(10):
            
            results = self.ending_digit_query(digit)

            # Append results to total dataframe
            # self.resulting_dataframe.append(results[0])
            # self.resulting_dataframe.append(results[1])

            # If the dataframe is not empty display it for orders that should have the given digit
            output_string += "\nThese orders should have an ending digit of " + str(digit)
            if results[0].empty:
                output_string += "\nNo orders need an ending digit of " + str(digit)
            else:
                output_string += results[0].loc[:, self.display_columns].to_string()

            # If the dataframe is not empty display it for orders that should not have the given digit
            output_string += "\nThese orders should not have an ending digit of " + str(digit)
            if results[1].empty:
                output_string += "\nNo wrong orders have an ending digit of " + str(digit)
            else:
                output_string += results[1].loc[:, self.display_columns].to_string()

   
        # Creates output file with above strings as text
        with open(r"C:\Users\cr003927\OneDrive - Maxar Technologies Holdings Inc\Private Drop\Git\Deck_Audit\Local_only\output.txt", 'w') as f:
            f.write(output_string)




queries = Queries()

