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
        self.excluded_customers = parameters["excluded customers"]
        self.display_columns = parameters["without_shapefile"]["columns to display"]
        self.columns_to_drop = parameters["without_shapefile"]["columns_to_drop"]
        self.spec_at_high_pri_input = parameters["query _pri_inputs"]["spec_at_high_pri"]

        # Create and clean the dataframe
        self.active_orders = self.create_dataframe()
        self.clean_dataframe(self.active_orders)

    def create_dataframe(self):
        """ Searches the map contents for the active orders layer and returns a dataframe from it """
        # Set variables to current project and map
        current_project = arcpy.mp.ArcGISProject("current")
        current_map = current_project.activeMap

        # Search layers for the active orders
        for layer in current_map.listLayers():
            if layer.isFeatureLayer:
                if layer.name == 'PROD_Active_Orders_UFP':
                    active_orders_layer = layer
                    break

        # Read the geo database table into pandas dataframe
        fields = [f.name for f in arcpy.ListFields(layer)]

        with arcpy.da.SearchCursor(layer, fields) as cursor:
            df = pd.DataFrame(list(cursor), columns=fields)

        return df

    def clean_dataframe(self, dataframe):
        """ Removes unnecessary fields from a given active_orders_ufp dataframe """

        new_df = dataframe.drop(labels=columns_to_drop, axis=1)

        return new_df[(new_df.tasking_priority > 690) & (~new_df.sap_customer_identifier.isin(['0000000306']) )]

    def spec_at_high_pri(self,dataframe, priority):
        """ Identifies orders that are spec responsiveness level and above the given priority """

        result = dataframe[(dataframe.responsiveness_level == 'None') & 
                        (dataframe.tasking_priority < priority) & 
                        (~dataframe.sap_customer_identifier.isin(self.excluded_customers["spec_at_high_pri"]))]
        
        return result
        

    def output(self):
        """ Creates a text file with the desired info """

        # Create string variables to print to file
        spec_at_high_pri = "\nSpec prioritized above " + str(self.spec_at_high_pri_input) + ":" + self.spec_at_high_pri(
                                                                                                        self.active_orders, 
                                                                                                        self.spec_at_high_pri_input).loc[:, self.display_columns].to_string()
    
        # Creates output file with above strings as text
        with open(r"C:\Users\ca003927\Music\Git\Deck_Audit\Local_only\output.txt", 'w') as f:
            f.write(spec_at_high_pri)
