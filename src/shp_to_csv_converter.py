"""
Extract shp attribute table into csv file removing the geometry column

Author: Chinmay Deval
"""

import os
import zipfile
import geopandas as gpd

def extract_zip(zip_path, extract_to):
    """Extract a zip file to the specified directory."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

def shapefiles_to_csv(folder_path):
    """Convert all shapefiles in the specified folder to CSV, excluding the geometry column."""
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.shp'):
                shp_path = os.path.join(root, file)
                # Read the shapefile using geopandas
                gdf = gpd.read_file(shp_path)
                # Drop the geometry column
                df = gdf.drop(columns='geometry')
                # Define the CSV file path
                csv_path = os.path.join(root, file.replace('.shp', '.csv'))
                # Save the dataframe to CSV
                df.to_csv(csv_path, index=False)

def main():
    zip_path = './HOGARES_POBLACION.zip'  # Path to your zip file
    extract_to = './HOGARES_POBLACION'    # Directory to extract to

    # Create the directory if it doesn't exist
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)

    # Extract the zip file
    extract_zip(zip_path, extract_to)

    # Convert shapefiles to CSV
    shapefiles_to_csv(extract_to)

if __name__ == "__main__":
    main()
