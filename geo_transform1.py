import os
import pandas as pd
import geopandas as gpd
import re
from shapely.geometry import Point
import numpy as np
import time

def automate_geodataframe(csv_file, output_dir, crs, geo_coords=['sent_locs']):
    """
    Create a GeoDataFrame from a CSV file containing belief and geographic coordinates.

    Args:
        csv_file (str): Path to the CSV file.
        output_dir (str): Directory to save the Shapefile and GeoJSON.
        crs (str): Coordinate Reference System (CRS) of the GeoDataFrame.
        geo_coords (list, optional): List of columns containing the geographic coordinates. Defaults to ['sent_locs'].
        columns_to_include (list, optional): List of columns to include in the resulting GeoDataFrame. If None, all columns are included.

    Returns:
        geopandas.GeoDataFrame: GeoDataFrame with specified columns and CRS.
    """
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)
    columns_to_include = df.columns

    if columns_to_include is None:
        columns_to_include = df.columns.tolist()  # Include all columns

    # Create an empty DataFrame
    data = pd.DataFrame(columns=columns_to_include)

    # Iterate over each row in the DataFrame
    for i, row in df.iterrows():
        regions = []
        coordinates = []

        # Iterate over the geo_coords columns
        for col in geo_coords:
            # Extract the coordinates for the current column
            geo_coords_col = row[col]

            # Skip if the column is empty or not a string
            if pd.isnull(geo_coords_col) or not isinstance(geo_coords_col, str):
                continue

            # Find the matches in the current column
            matches = re.findall(r'([A-Za-z ]+)\s*\(([-+]?\d+\.\d+),\s*([-+]?\d+\.\d+)\)', geo_coords_col)

            # Append the matches to the regions and coordinates lists
            for match in matches:
                region = match[0]
                lat, lon = map(float, match[1:])
                regions.append(region)
                coordinates.append((lon, lat))

        # Select unique regions per row
        unique_regions = list(set(regions))

        # Process each unique region
        for region in unique_regions:
            # Find the corresponding coordinates for the unique region
            region_coords = [coord for coord, reg in zip(coordinates, regions) if reg == region]

            # Create a Point geometry for each set of coordinates
            geometry = Point(region_coords[0])  # Assuming you want only the first set of coordinates
            # Create a Point geometry for each set of coordinates
            geometries = [Point(lon, lat) for lon, lat in region_coords]

            for geometry in geometries:
                # Create a new row in the DataFrame with the specified columns
                new_row = {col: row[col] for col in columns_to_include}
                
#                 new_row = {
# #                     'ID': ids[0],  # Assuming you want to use the first ID
#                     'region': region,
#                     'geometry': geometry
#                 }
                
                

                
                new_row['location']= region
                new_row['geometry'] = geometry
                new_df = pd.DataFrame([new_row])
                # Append the new row to the data DataFrame
                if (not new_df.empty):
                    data= pd.concat([data, new_df], ignore_index=True) 
#             data = pd.concat([data, new_df], ignore_index=True)

#     data.to_csv(os.path.join(output_dir, 'CausalBeliefsDate_converted.csv'))
    # Create a GeoDataFrame from the DataFrame and assign CRS
    gdf = gpd.GeoDataFrame(data, crs=crs)

    # Create a GeoDataFrame from the DataFrame and assign CRS
#     gdf = gpd.GeoDataFrame(data, crs=4326)

    gdf =gdf.to_crs(crs)
    # Rename columns as needed
#     gdf = gdf.rename(columns={'region': 'location', 'LOCATION': 'LOCATION_'})

    # Save to CSV and GeoJSON
    gdf.to_csv(os.path.join(output_dir, 'output.csv'))
    gdf.to_file(os.path.join(output_dir, 'output.geojson'), driver='GeoJSON')

    return gdf

# Example usage:
if __name__ == "__main__":

    csv_file = 'path/to/data.csv'
    output_dir = 'path/to/output/'
    crs = 'EPSG:4326'
    geo_coords = ['sent_locs', 'context_locs']
    # columns_to_include = ['url', 'terms', 'date', 'sentenceIndex', 'sentence', 'causal', 'causalIndex', 'negationCount', 'causeIncCount', 'causeDecCount', 'causePosCount', 'causeNegCount', 'effectIncCount', 'effectDecCount', 'effectPosCount', 'effectNegCount', 'causeText', 'effectText', 'belief', 'sent_locs', 'context_locs', 'canonicalDate']

    # Record the start time
    start_time = time.time()
    print(start_time)
    geodf = automate_geodataframe(csv_file, output_dir, crs, geo_coords= geo_coords)
    # Record the end time
    end_time = time.time()
    # Calculate and print the runtime
    runtime = end_time - start_time
    print(f"Runtime of my_function: {runtime} seconds")
