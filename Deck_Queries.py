# This file contains the queries used to interrogate the tasking deck and will return the results

import arcpy
import pandas as pd
import json
from pathlib import Path

parameters_path = Path(r"C:\Users\ca003927\Music\Git\Deck_Audit\Local_only\Sensitive_Parameters.json")

with open(parameters_path, 'r') as input:
    parameters = json.load(input)


class Queries():
    """ Contains the qureie and output functions needed for the deck audit """

    def __init__(self) -> None:
        """ Creates dataframe and sets varables """

        # define parameter variables
        self.display_columns = parameters["without_shapefile"]["columns to display"]
        self.columns_to_drop = parameters["without_shapefile"]["columns_to_drop"]
        self.query_input = parameters["query_inputs"]
        self.arc_project_loc = parameters["arc_project_path"]
        self.arc_map_name = parameters["arc_map_name"]
        self.idi_customers = parameters["IDI_customers"]

        # Create and clean the dataframe
        self.active_orders = self.create_dataframe()
        self.clean_dataframe(self.active_orders)
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

    def clean_dataframe(self, dataframe):
        """ Removes unnecessary fields from a given active_orders_ufp dataframe """

        new_df = dataframe.drop(labels=self.columns_to_drop, axis=1)

        return new_df[(new_df.tasking_priority > 690) & (~new_df.sap_customer_identifier.isin(['0000000306']) )]

    def orders_at_high_pri(self, responsiveness):
        """ Identifies orders of the given responsiveness that are below the appropreate priority """

        return self.active_orders[
                        (self.active_orders.responsiveness_level == responsiveness) & 
                        (self.active_orders.tasking_priority < self.query_input["orders_at_high_pri"][responsiveness]["pri"]) & 
                        (~self.active_orders.sap_customer_identifier.isin(self.query_input["orders_at_high_pri"][responsiveness]["excluded_cust"]))]
    
    def orders_at_low_pri(self, responsiveness):
        """ Identifies orders of the given responsiveness that are above the appropreate priority """

        return self.active_orders[
                        (self.active_orders.responsiveness_level == responsiveness) & 
                        (self.active_orders.tasking_priority > self.query_input["orders_at_low_pri"][responsiveness]["pri"]) & 
                        (~self.active_orders.sap_customer_identifier.isin(self.query_input["orders_at_low_pri"][responsiveness]["excluded_cust"]))]
    
    def idi_not_ending_in_1(self):
        """ Identifies orders that should have 1 as the final digit """

        return self.active_orders[
                        (self.active_orders.sap_customer_identifier.isin(self.idi_customers.values())) &
                        (~self.active_orders.tasking_priority.isin([701, 711, 721, 731, 741, 751, 761, 771, 781, 791, 801]))
        ]
    
    def not_idi_ending_in_1(self):
        """ Identifies orders that should not have 1 as the final digit """

        return self.active_orders[
                        (~self.active_orders.sap_customer_identifier.isin(self.idi_customers.values())) &
                        (self.active_orders.tasking_priority.isin([701, 711, 721, 731, 741, 751, 761, 771, 781, 791, 801]))
        ]
    def internal_not_ending_in_2(self):
        """ Identifies orders that should have 2 as the final digit """

        return self.active_orders[
                (self.active_orders.sap_customer_identifier.isin(self.query_input["internal_not_ending_in_2"]["included_cust"])) &
                (~self.active_orders.tasking_priority.isin([702, 712, 722, 732, 742, 752, 762, 772, 782, 792, 802]))
        ]
    
    def ending_digit_query(self, digit):
        """ Returns two dataframes, one for orders that should have the given ending digit, but don't, and one for orders that shouldn't have the given ending digit, but do """

        pri_list = [x + digit for x in range(700,810,10)]

        if digit in [1,2,8,9]:

            should = self.active_orders[
                                        # customers to include (if any)
                                        self.active_orders.sap_customer_identifier.isin(self.query_input["ending_digit_cust_list"][str(digit)]) &
                                        # priorities that orders should have
                                        ~self.active_orders.tasking_priority.isin(pri_list)
                                    ]

            should_not = self.active_orders[
                                        # customers to exclude (if any)
                                        ~self.active_orders.sap_customer_identifier.isin(self.query_input["ending_digit_cust_list"][str(digit)]) &
                                        # priorities that orders should have
                                        self.active_orders.tasking_priority.isin(pri_list)
                                    ]
            
        if digit == 3:

            should = self.active_orders[ 
                                        # Order is not active on any spacecraft but WV03
                                        (self.active_orders.ge01 == 0) & (self.active_orders.wv01 == 0) & (self.active_orders.wv02 == 0) &
                                        # Order is not in the customer group
                                        ~self.active_orders.sap_customer_identifier.isin(self.query_input["ending_digit_cust_list"][str(digit)]) &
                                        # Order priority does not end in 3
                                        ~self.active_orders.tasking_priority.isin(pri_list)
            ]

            should_not = pd.DataFrame()

        if digit == 4:

            should = self.active_orders[ 
                                        # Order is active on more then one spacecraft
                                        ((self.active_orders.ge01 == 1) | (self.active_orders.wv01 == 1) | (self.active_orders.wv02 == 1)) &
                                        # Order is not in the customer group
                                        ~self.active_orders.sap_customer_identifier.isin(self.query_input["ending_digit_cust_list"][str(digit)]) &
                                        # Order priority does not end in 3
                                        ~self.active_orders.tasking_priority.isin(pri_list)
            ]

            should_not = pd.DataFrame()

        if digit in [0,5,6,7]:

            should = pd.DataFrame()
            should_not = pd.DataFrame()
        
        return [should, should_not]
                                   
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
            arcpy.AddMessage("Running query for ending digit: " + str(digit))
            results = self.ending_digit_query(digit)

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
        with open(r"C:\Users\ca003927\Music\Git\Deck_Audit\Local_only\output.txt", 'w') as f:
            f.write(output_string)


queries = Queries()

