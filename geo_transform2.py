import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from concurrent.futures import ProcessPoolExecutor
import time

def map_process_row(row):
    regions = []
    coordinates = []
    ids = []
    
    for col in geo_coords:
        geo_coords_col = row[col]
        
        if pd.isnull(geo_coords_col) or not isinstance(geo_coords_col, str):
            continue
        
        matches = re.findall(r'([A-Za-z ]+)\s*\(([-+]?\d+\.\d+),\s*([-+]?\d+\.\d+)\)', geo_coords_col)
        
        for match in matches:
            region = match[0]
            lat, lon = map(float, match[1:])
            regions.append(region)
            coordinates.append((lon, lat))
            ids.append(row['ID'])
    
    return ids, coordinates, regions

def process_row(row,reduce_data, original_columns):
    ids, coordinates, regions = reduce_data
    
    unique_regions = list(set(regions))
    
    # Process each unique region
    rows = []
    for region in unique_regions:
        region_coords = [coord for coord, reg in zip(coordinates, regions) if reg == region]
        geometry = Point(region_coords[0])
        
        new_row = {
            'ID': ids[0],  # Assuming you want to use the first ID
            'region': region,
            'geometry': geometry
        }
        
        # Include rows for other existing columns from the original DataFrame
        for col in original_columns:
            new_row[col] = row[col] #* len(unique_regions)
        
        rows.append(new_row)
    
    return rows


def create_geodataframe_map_reduce(csv_file, output_dir, crs, belief='sentence', geo_coords=['sent_locs']):
    df = pd.read_csv(csv_file)
    original_columns = df.columns.tolist()  # Store the original columns
    
    data_list = df.to_dict(orient='records')
    
    # Use map to process rows in parallel
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        mapped_data = list(executor.map(map_process_row, data_list))
    
#     print(mapped_data[:10])
    # Combine the results from map
#     data = []
#     for reduce_data in mapped_data:
#         data.extend(process_row(reduce_data, original_columns))
    data = []
    for i, reduce_data in enumerate(mapped_data):
        data.extend(process_row(data_list[i], reduce_data, original_columns))


    # Create a DataFrame from the processed data
    data_df = pd.DataFrame(data)
    
    # Create a GeoDataFrame from the DataFrame and assign CRS
    gdf = gpd.GeoDataFrame(data_df, crs=4326)

    # Convert CRS
    gdf = gdf.to_crs(crs)
    gdf = gdf.rename(columns={'region': 'location'})

    
    # Save to CSV and GeoJSON
    gdf.to_csv(os.path.join(output_dir, 'output.csv'))
    gdf.to_file(os.path.join(output_dir, 'output.geojson'), driver='GeoJSON')

    return gdf

if __name__ == "__main__":
    # Record the start time
    start_time = time.time()
    csv_file = 'path/to/data.csv'
    output_dir = 'path/to/output/'
    crs = 'EPSG:4326'
    geo_coords = ['sent_locs', 'context_locs']

    geodf1 = create_geodataframe_map_reduce(csv_file, output_dir, crs, geo_coords= geo_coords)
    
    # Record the end time
    end_time = time.time()
    # Calculate and print the runtime
    runtime = end_time - start_time
    print(f"Runtime of my_function: {runtime} seconds")
