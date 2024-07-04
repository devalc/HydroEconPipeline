"""
## --------------------------------------------------------------------------------------##
##
## Script name: fetch_chirps_ts_from_climateserv.py
##
## Purpose of the script: Download CHIRPS timeseries data for all polygons with-in a given shapefile
## Author: Chinmay Deval
##
## Created On: 06/14/2024
##
## --------------------------------------------------------------------------------------##
"""

# Import required packages
import os
import pandas as pd
import climateserv.api
import geopandas as gpd
from datetime import datetime, timedelta

# Path to geomerty
path_to_geom = './Pib_provincias_2020/Pib_provincias_2020.shp'

# read geometry file
geomdf = gpd.read_file(path_to_geom)

# Print the count of polygons
polygon_count = len(geomdf)
print(f'Total number of polygons in the shapefile: {polygon_count}')

# Print the original CRS (Coordinate Reference System)
original_crs = geomdf.crs
print(f'Original CRS: {geomdf.crs}')

# Reproject to WGS84 (latitude/longitude) if necessary
if original_crs != 'EPSG:4326':
    geomdf = geomdf.to_crs(epsg=4326)
    print('Reprojected to WGS84 (EPSG:4326)')
else:
    print('Already in WGS84 (EPSG:4326)')

# Simplify the polygons to reduce the number of vertices
# The tolerance parameter controls the level of simplification
tolerance = 0.01  # Adjust this value as needed
geomdf['geometry'] = geomdf['geometry'].simplify(tolerance, preserve_topology=True)

# Function to convert polygon coordinates to ClimateSERV format
def get_geometry_coords(polygon):
    if not polygon.exterior.is_ring:
        coords = list(polygon.exterior.coords)
        if coords[0] != coords[-1]:
            coords.append(coords[0])
    else:
        coords = list(polygon.exterior.coords)
    return [[x, y] for x, y in coords]

def convert_multipolygons_to_polygons(gpdf):
    single_polygons = gpdf[gpdf.geometry.type == 'Polygon']
    multipolygons = gpdf[gpdf.geometry.type == 'MultiPolygon']

    for i, row in multipolygons.iterrows():
        polygon_series = pd.Series(row.geometry)
        row_expanded = pd.concat([gpd.GeoDataFrame(row).T] * len(polygon_series), ignore_index=True)
        row_expanded = gpd.GeoDataFrame(row_expanded, crs=gpdf.crs)
        row_expanded['geometry'] = polygon_series
        single_polygons = pd.concat([single_polygons, row_expanded])

    single_polygons.reset_index(inplace=True, drop=True)
    return single_polygons

def generate_5yr_intervals(start_date, end_date, chunk_size_years=5):
    intervals = []
    current_start = datetime.strptime(start_date, '%m/%d/%Y')
    end_date_dt = datetime.strptime(end_date, '%m/%d/%Y')

    while current_start.year + chunk_size_years <= end_date_dt.year:
        current_end = current_start.replace(year=current_start.year + chunk_size_years - 1, month=12, day=31)
        intervals.append((current_start.strftime('%m/%d/%Y'), current_end.strftime('%m/%d/%Y')))
        current_start = current_end + timedelta(days=1)
        current_start = current_start.replace(day=1, month=1)

    # Add the last interval
    if current_start <= end_date_dt:
        intervals.append((current_start.strftime('%m/%d/%Y'), end_date_dt.strftime('%m/%d/%Y')))

    return intervals

DatasetType = 0  # UCSB CHIRPS Rainfall: 0
OperationType = 'Average'
EarliestDate = '01/01/1981'  # Example start date
LatestDate = '06/10/2024'    # Example end date
SeasonalEnsemble = ''  # Leave empty when using the new integer dataset IDs
SeasonalVariable = ''  # Leave empty when using the new integer dataset IDs

geomdf_multpart = convert_multipolygons_to_polygons(geomdf)

intervals = generate_5yr_intervals(EarliestDate, LatestDate)

# Iterate over each polygon in the GeoDataFrame
for index, row in geomdf_multpart.iterrows():
    polygon = row['geometry']
    province_name = row['provincias']
    province_number = row['DPA_PROVIN']

    FinalOutfile = f'out_{province_name}_{index}.csv'
    if os.path.exists(FinalOutfile):
        print(f'Final output file {FinalOutfile} already exists. Skipping processing for province: {province_name} at index {index}.')
        continue

    if polygon.geom_type == 'Polygon':
        GeometryCoords = get_geometry_coords(polygon)
        print(f'Processing Polygon for province: {province_name} at index {index}')
    elif polygon.geom_type == 'MultiPolygon':
        print(f'Skipping MultiPolygon geometry for province: {province_name} at index {index}')
        continue
    else:
        print(f'Skipping non-polygon geometry for province: {province_name} at index {index}')
        continue

    all_data = []

    # Generate date intervals and request data for each interval
    date_intervals = generate_5yr_intervals(EarliestDate, LatestDate)
    for start_date, end_date in date_intervals:
        Outfile = f'temp_{province_name}_{index}_{start_date.replace("/", "-")}_{end_date.replace("/", "-")}.csv'
        
        # Check if the temporary file already exists
        if os.path.exists(Outfile):
            print(f'Temporary file {Outfile} already exists. Skipping download.')
        else:
            climateserv.api.request_data(DatasetType, OperationType, 
                                         start_date, end_date, GeometryCoords, 
                                         SeasonalEnsemble, SeasonalVariable, Outfile)
            print(f'Data requested for province {province_name} (index {index}) from {start_date} to {end_date} and saved to {Outfile}')
        
        if os.path.exists(Outfile):
            df = pd.read_csv(Outfile, skiprows=1, header=0)
            df = df.dropna()
            df['Province'] = province_name
            df['DPA_PROVIN'] = province_number
            all_data.append(df)
            os.remove(Outfile)
        else:
            print(f'File {Outfile} does not exist. Skipping processing for interval {start_date} to {end_date}.')

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(FinalOutfile, index=False)
        print(f'All data for province {province_name} (index {index}) saved to {FinalOutfile}')
    else:
        print(f'No data collected for province {province_name} (index {index}).')
