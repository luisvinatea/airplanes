import pandas as pd
import geopandas as gpd
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set the data dump directory from the environment variable
data_dump_dir = os.getenv('DEV_DUMP_DIR', '/mnt/data/dev_dumps')

# Define file paths within the data dump directory
airlines_file = os.path.join(data_dump_dir, 'clean_airline_data.csv')
country_codes_file = os.path.join(data_dump_dir, 'clean_country_codes.csv')
world_shapefile = os.path.join(data_dump_dir, 'ne_110m_admin_0_countries.shp')
output_file = os.path.join(data_dump_dir, 'airlines_with_country.csv')

# Load CSV
airlines_df = pd.read_csv(airlines_file)

# Clean and normalize IATA codes
airlines_df['IATA'] = airlines_df['IATA'].str.strip().str.upper()

# Extract country codes from ICAO
# ICAO codes: first two letters represent the country
airlines_df['Country Code'] = airlines_df['ICAO'].str[:2].str.upper()

# Load world shapefile and country codes
world = gpd.read_file(world_shapefile)
country_codes = pd.read_csv(country_codes_file)

# Join world data with country codes
global_country_data = world.merge(country_codes, left_on='ISO_A2', right_on='ISO2', how='left')

# Merge airlines with country data based on the extracted country codes
airlines_with_country = airlines_df.merge(country_codes, left_on='Country Code', right_on='ISO2', how='left')

# Save the updated dataframe to CSV
airlines_with_country.to_csv(output_file, index=False)

# Diagnostic output
logging.info(airlines_with_country.head())
logging.info(f"Total airlines processed: {len(airlines_with_country)}")
