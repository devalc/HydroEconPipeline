## --------------------------------------------------------------------------------------##
##
## Script name: aggregate_chirps_daily_ts_from_climateserv_to_monthly.py
##
## Purpose of the script: Creates monthly total precip files for daily values downloaded from climateserv
## Author: Chinmay Deval
##
## Created On: 06/18/2024
##
## --------------------------------------------------------------------------------------##

import pandas as pd
import glob
import os

# Path to the directory containing the CSV files
input_directory = './'
output_directory = './Monthly/'

# Get a list of all CSV files in the input directory
csv_files = glob.glob(os.path.join(input_directory, 'out_*.csv'))

# Process each CSV file
for file in csv_files:
    # Load the data
    df = pd.read_csv(file)
    
    # Strip any leading/trailing spaces from column names
    df.columns = df.columns.str.strip()
    
    # Print column names for debugging
    print(f"Processing file: {file}")
    print("Column names:", df.columns.tolist())
    
    # Check if 'date' column exists
    if 'date' not in df.columns:
        print(f"Error: 'date' column not found in {file}")
        continue
    
    # Convert the date column to datetime
    df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')
    
    # Create a new column for year and month
    df['year_month'] = df['date'].dt.to_period('M')
    
    # Group by year_month, Province, and DPA_PROVIN, then calculate the sum of precipitation
    monthly_totals = df.groupby(['year_month', 'Province', 'DPA_PROVIN'])['avg'].sum().reset_index()
    
    # Ensure the year_month is of period dtype
    monthly_totals['year_month'] = monthly_totals['year_month'].astype('period[M]')
    
    # Convert year_month to datetime format representing the last day of each month
    monthly_totals['date'] = monthly_totals['year_month'].dt.to_timestamp(how='end').dt.date
    
    # Rename the columns
    monthly_totals.columns = ['year_month', 'Province', 'DPA_PROVIN', 'monthly_total_precipitation', 'date']
    
    # Drop the year_month column as it's now redundant
    monthly_totals = monthly_totals.drop(columns=['year_month'])
    
    # Extract the file name without extension
    file_name = os.path.splitext(os.path.basename(file))[0]
    
    # Save the result to a new CSV file
    output_file = os.path.join(output_directory, f'{file_name}_monthly_totals.csv')
    monthly_totals.to_csv(output_file, index=False)

print('Monthly total precipitation CSV files have been created.')
