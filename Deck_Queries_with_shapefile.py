# This file contains the queries used to interrogate the tasking deck and will return the results
# Requires shapefile (.dbf)

import json
import pandas as pd
import geopandas as gpd 
from pathlib import Path

# Load .json containing sensitive parameters
with open('Local_only\Sensitive_Parameters.json', 'r') as input:
    parameters = json.load(input)

# define parameter variables
excluded_customers = parameters["excluded customers"]
display_columns = parameters["with_shapefile"]["columns to display"]
columns_to_drop = parameters["with_shapefile"]["columns_to_drop"]

# Path to the active orders UFP
active_orders_path = Path("Local_only\PROD_Active_Orders_UFP_pri690-800.dbf")

def create_dataframe(source_file_path):
    """ Reads the .pkl file into a pandas dataframe """ 

    pickle_path = Path("Local_only\orders_dataframe.pkl")

    if pickle_path.is_file():
        df = pd.read_pickle(pickle_path)
    else:
        # if the pickel file does not exist then create it from the active ordrs .dbf file
        df = pd.DataFrame(gpd.read_file(source_file_path))
        clean_df = clean_dataframe(df)
        clean_df.to_pickle(pickle_path)

    return df

def clean_dataframe(dataframe):
    """ Removes unnecessary filedls from a given active_orders_ufp dataframe """

    new_df = dataframe.drop(labels=columns_to_drop, axis=1)

    return new_df[new_df.tasking_pr > 690]


def spec_at_high_pri(dataframe, priority):
    """ Identifies orders that are spec responsiveness level and above the given priority """

    excluded_cust = excluded_customers["spec_at_high_pri"]

    result = dataframe[(dataframe.responsive == 'None') & (dataframe.tasking_pr < priority) & (~dataframe.sap_custom.isin(excluded_cust))]
    
    print("\nSpec prioritized above ", priority, ":")
    print( result.loc[:, display_columns] )


def select_at_high_pri(dataframe, priority):
    """ Identifies orders that are select responsiveness level and above the given priority """

    excluded_cust = excluded_customers["select_at_high_pri"]

    result = dataframe[(dataframe.responsive == 'Select') & (dataframe.tasking_pr < priority) & (~dataframe.sap_custom.isin(excluded_cust))]
    
    print("\nSelect prioritized above ", priority, ":")
    print( result.loc[:, display_columns] )

def selectplus_at_high_pri(dataframe, priority):
    """ Identifies orders that are selectplus responsiveness level and above the given priority """

    excluded_cust = excluded_customers["selectplus_at_high_pri"]

    result = dataframe[(dataframe.responsive == 'SelectPlus') & (dataframe.tasking_pr < priority) & (~dataframe.sap_custom.isin(excluded_cust))]
    
    print("\nSelectPlus prioritized above ", priority, ":")
    print( result.loc[:, display_columns] )

def selectplus_at_low_pri(dataframe, priority):
    """dentifies orders that are selectplus responsiveness level and above the given priority"""

    excluded_cust = excluded_customers["selectplus_at_low_pri"]

    result = dataframe[(dataframe.responsive == 'SelectPlus') & (dataframe.tasking_pr > priority) & (~dataframe.sap_custom.isin(excluded_cust))]

    print("\nSelectPlus prioritized below ", priority, ":")
    print( result.loc[:, display_columns] )
    return result.loc[:, display_columns]

def output_file(dataframe):
    """ Creates a text file with the desired info """

    with open('output.txt', 'w') as f:
        f.write(dataframe)

if __name__ == "__main__":
    orders = create_dataframe(active_orders_path)


    spec_at_high_pri(orders, 750)
    select_at_high_pri(orders, 720)
    selectplus_at_high_pri(orders, 710)
    output_file(selectplus_at_low_pri(orders,750).to_string())
    

