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

    def output(self):
        """ Creates a text file with the desired info """

        output_string = ""

        for query in ["high", "low"]:
            for responsiveness in ['None', 'Select', 'SelectPlus']:
                if query == "high": func = self.orders_at_high_pri
                else: func = self.orders_at_low_pri
                output_string += "\nThese " + responsiveness + " orders may be too " + query + func(responsiveness).loc[:, self.display_columns].to_string()
    
        # Creates output file with above strings as text
        with open(r"C:\Users\ca003927\Music\Git\Deck_Audit\Local_only\output.txt", 'w') as f:
            f.write(output_string)


queries = Queries()

