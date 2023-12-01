How to use optimised packages for geo-processing ?

# Code Documentation: Geospatial Data Transformation

## Overview

This repository contains three versions of geospatial data transformation code, each implemented with different approaches and optimizations. The goal is to convert a CSV file containing belief and geographic coordinates into a GeoDataFrame, incorporating parallel processing, Dask, and Pandas. The geographical coordinates are embedded within specific columns.

### Sample Input Data

Here is an example of the input data in the CSV format:

| belief | date           | sent_locs                                          | context_locs                                       | ID   |
|--------|----------------|----------------------------------------------------|----------------------------------------------------|------|
| False  | June 3, 2023    | Kampala (0.00687, 31.07689), Uganda (1.25, 32.5)  | Kampala (0.00687, 31.07689), Uganda (1.25, 32.5)  | 63739|
| False  | June 3, 2023    | NaN                                                | Kampala (0.00687, 31.07689), Uganda (1.25, 32.5)  | 63740|
| True   | June 3, 2023    | NaN                                                | Kampala (0.00687, 31.07689), Uganda (1.25, 32.5)  | 63741|
| False  | June 3, 2023    | NaN                                                | Kampala (0.00687, 31.07689), Uganda (1.25, 32.5)  | 63742|
| False  | June 3, 2023    | NaN                                                | NaN                                                | 63743|

This sample data includes columns such as `belief`, `date`, `sent_locs`, `context_locs`, and `ID`. Use this format as a reference for the input data used in the geospatial data transformation code.


### Sample Output Data

Here is an example of the output data after geospatial transformation:

| belief | date           | sent_locs                                          | context_locs                                       | ID   | location | geometry                |
|--------|----------------|----------------------------------------------------|----------------------------------------------------|------|----------|-------------------------|
| False  | June 3, 2023    | Kampala (0.00687, 31.07689), Uganda (1.25, 32.5)  | Kampala (0.00687, 31.07689), Uganda (1.25, 32.5)  | 63739| Uganda   | POINT (32.50000 1.25000)|
| False  | June 3, 2023    | Kampala (0.00687, 31.07689), Uganda (1.25, 32.5)  | Kampala (0.00687, 31.07689), Uganda (1.25, 32.5)  | 63739| Kampala  | POINT (31.07689 0.00687)|
| False  | June 3, 2023    | None                                               | Kampala (0.00687, 31.07689), Uganda (1.25, 32.5)  | 63740| Uganda   | POINT (32.50000 1.25000)|
| False  | June 3, 2023    | None                                               | Kampala (0.00687, 31.07689), Uganda (1.25, 32.5)  | 63740| Kampala  | POINT (31.07689 0.00687)|
| True   | June 3, 2023    | None                                               | Kampala (0.00687, 31.07689), Uganda (1.25, 32.5)  | 63741| Uganda   | POINT (32.50000 1.25000)|

This sample output data includes additional columns `location` and `geometry` after performing the geospatial transformation. 

The transformation process involves parsing the original location information, extracting coordinates, creating Point geometries, and then associating each unique location with the corresponding geometry. The result is a GeoDataFrame suitable for geospatial analysis and visualization.


## Versions

### Version 1: [Geo Transform v1](https://github.com/glods/geospatial_data_transformation/blob/main/geo_transform1.py)

- **Packages:**
  - `pandas`
  - `geopandas`
  - `shapely`
  - `time`

- **Approach:**
  - Sequential processing with Pandas and GeoPandas.

- **Performance:**
  - Execution Time: > 1h

### Example Usage

```python
  from geo_transform1 import automate_geodataframe
  csv_file = '/path/to/data.csv'
  output_dir = '/path/to/output/'
  crs = 'EPSG:4326'
  geo_coords = ['sent_locs', 'context_locs']
  
  # Record the start time
  start_time = time.time()
  geodf = automate_geodataframe(csv_file, output_dir, crs, geo_coords= geo_coords)
  # Record the end time
  end_time = time.time()
  # Calculate and print the runtime
  runtime = end_time - start_time
  print(f"Runtime of automate_geodataframe: {runtime} seconds")
```

### Version 2:[ Geo Transform v2](https://github.com/glods/geospatial_data_transformation/blob/main/geo_transform2.py)

- **Packages:**
  - `pandas`
  - `geopandas`
  - `shapely`
  - `concurrent.futures`
  - `time`

- **Approach:**
  - Parallel processing using `concurrent.futures.ProcessPoolExecutor`.

- **Performance:**
  - Execution Time: 34 seconds
### Example Usage
``` Python
from geo_transform2 import create_geodataframe_map_reduce
csv_file = '/path/to/data.csv'
output_dir = '/path/to/output/'
crs = 'EPSG:4326'
geo_coords = ['sent_locs', 'context_locs']

# Record the start time
start_time = time.time()
geodf_v2 = create_geodataframe_map_reduce(csv_file, output_dir, crs, geo_coords)
# Record the end time
end_time = time.time()
# Calculate and print the runtime
runtime = end_time - start_time
print(f"Runtime of create_geodataframe_map_reduce: {runtime} seconds")
```

### Version 3: [Geo Transform v3 (Dask Version)](https://github.com/glods/geospatial_data_transformation/blob/main/geo_transform1.py)

- **Packages:**
  - `dask`
  - `pandas`
  - `geopandas`
  - `shapely`
  - `time`

- **Approach:**
  - Utilizes Dask for parallel processing, suitable for large datasets.

- **Performance:**
  - Execution Time: 27s

### Example Usage
``` Python
from geo_transform3 import create_geodataframe_dask
csv_file = '/path/to/data.csv'
output_dir = '/path/to/output/'
crs = 'EPSG:4326'
geo_coords = ['sent_locs', 'context_locs']

# Record the start time
start_time = time.time()
geodf_v3 = create_geodataframe_dask(csv_file, output_dir, crs, geo_coords= geo_coords)
# Record the end time
end_time = time.time()
# Calculate and print the runtime
runtime = end_time - start_time
print(f"Runtime of create_geodataframe_dask: {runtime} seconds")
```

## Conclusion

- Version 3 demonstrates the fastest processing time, suitable for large datasets.
- Version 2 introduces parallelism but has higher execution time than Version 3.
- Version 1 provides a simple approach with the highest execution time, suitable for smaller datasets.

Consider choosing the version that best aligns with the dataset size and processing requirements.
