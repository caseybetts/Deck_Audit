# Deck Audit (Rivedo)

## How to use
Within the Catalog pane, navigate to the Deck_Audit folder, click the toolbox dropdown and double-click the tool. The tool reqires two layers as parameters: 
1. Orders Layer - layer can be entier order deck or queried to a subset of orders
2. Hotlist Layer - this layer should not have a definition query

One or both of the two output options can be selected:
1. Local - this will output a layer to the project's default geodatabase, add the layer to the map, and apply symbology matching the input orders layer
2. SharePoint - this will output a layer to a shared folder which can be pulled into the map and multiple users can use simultaneously 
Once parameter fields are populated and the desired output location(s) are selected, click 'Run'.

## Output
If the Local output was selected then a feature class will be created in the project's default geodatabase and will be added as a new layer to the map. The symbology of the input orders layer will be applied to this layer. If the Sharepoint output was selected a new shapefile
will be added to the shared location. If a shapefile exist in that locaiton then it will be deleted and replaced with the new files. Users who have layers sourced to the existing files will not see any disruption and their layers will update with the new data automatically.
The new featuer class will have field names truncated to ten digits. The new feature class will have several new fields:
- Rivedo_Pri - the suggested priority for the order based on customer, spacecraft, order description and order PO
- End_Digit - this is either 'Y' or 'N' depending on if the ending digit of the tasking priority matches the ending digit of the suggested priority
- Mid_Digit - this is either 'Y' or 'N' depending on if the middle digit of the tasking priority matches the middle digit of the suggested priority
- High_Low - this flaggs an order as 'High' if the tasking priority is unusually high, 'Low' if it is unusually low, 'Standard' if it is not unusually high or low, and 'Excluded' if the order has been excluded from this check

