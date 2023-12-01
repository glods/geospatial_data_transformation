import os
import time
import dask
import dask.dataframe as dd
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import re

# def map_process_row(row, geo_coords):
#     regions = []
#     coordinates = []
#     ids = []
    
#     for col in geo_coords:
#         geo_coords_col = row[col]
        
#         if pd.isnull(geo_coords_col) or not isinstance(geo_coords_col, str):
#             continue
        
#         matches = re.findall(r'([A-Za-z ]+)\s*\(([-+]?\d+\.\d+),\s*([-+]?\d+\.\d+)\)', geo_coords_col)
        
#         for match in matches:
#             region = match[0]
#             lat, lon = map(float, match[1:])
#             regions.append(region)
#             coordinates.append((lon, lat))
#             ids.append(row['ID'])
    
#     return ids, coordinates, regions
def map_process_row(row, geo_coords):
    regions = []
    coordinates = []
    ids = []
    
    for col in geo_coords:
        geo_coords_col = row[col]
        
        if pd.isnull(geo_coords_col) or not isinstance(geo_coords_col, str):
            continue
        
        matches = re.findall(r'([A-Za-z ]+)\s*\(([-+]?\d+\.\d+),\s*([-+]?\d+\.\d+)\)', geo_coords_col)
        
#         if not matches:
#             print(f"No matches found for row {row}")
        
        for match in matches:
            region = match[0]
            lat, lon = map(float, match[1:])
            regions.append(region)
            coordinates.append((lon, lat))
            ids.append(row['ID'])
    
    return ids, coordinates, regions



def process_row_dask(row, reduce_data, original_columns):
    ids, coordinates, regions = reduce_data
    
    unique_regions = list(set(regions))
    
    rows = []
    for region in unique_regions:
        region_coords = [coord for coord, reg in zip(coordinates, regions) if reg == region]
        geometry = Point(region_coords[0])
        
        new_row = {
            'ID': ids[0],
            'region': region,
            'geometry': geometry
        }
        
        for col in original_columns:
            new_row[col] = row[col]
        
        rows.append(new_row)
    
    return rows

def create_geodataframe_dask(csv_file, output_dir, crs, belief='sentence', geo_coords=['sent_locs']):
    # Use dask to read the CSV file in parallel
    dtype={'causeText': 'object', 'effectText': 'object'}
    df = dd.read_csv(csv_file, dtype=dtype)
    
    # Print information about data before mapping
    print("Data before mapping:")
    print(df.head())
    
    # Trigger computation and convert to pandas DataFrame
    df = df.compute()

    original_columns = df.columns.tolist()
    data_list = df.to_dict(orient='records')

    data = []
#     for i, reduce_data in enumerate(map(lambda x: map_process_row(x, geo_coords), data_list)):
#         data.extend(process_row_dask(data_list[i], reduce_data, original_columns))

    # Print information about processed data
    print("Processed data:")
    for i, reduce_data in enumerate(map(lambda x: map_process_row(x, geo_coords), data_list)):
#         print(f"Mapping result for row {i}: {reduce_data}")
        data.extend(process_row_dask(data_list[i], reduce_data, original_columns))


    data_df = pd.DataFrame(data)
    
    # Print information about processed data
    print("Processed data:")
    print(data_df.head())
    
    gdf = gpd.GeoDataFrame(data_df, crs=4326)

    gdf = gdf.to_crs(crs)
    gdf = gdf.rename(columns={'region': 'location'})

    gdf.to_csv(os.path.join(output_dir, 'output.csv'))
    gdf.to_file(os.path.join(output_dir, 'output.geojson'), driver='GeoJSON')

    return gdf

if __name__ == "__main__":
    start_time = time.time()
    
    csv_file = 'path/to/data.csv'
    output_dir = 'path/to/output/'
    crs = 'EPSG:4326'
    geo_coords = ['sent_locs', 'context_locs']

    # Set the number of processes for parallelism
    dask.config.set(scheduler='processes', num_workers=os.cpu_count())

    geodf1 = create_geodataframe_dask(csv_file, output_dir, crs, geo_coords= geo_coords )

    end_time = time.time()
    runtime = end_time - start_time
    print(f"Runtime: {runtime} seconds")
