import pandas as pd

# Load all datasets
airline_data = pd.read_csv("data/raw/airline_data.csv", header=None)
airplane_models = pd.read_csv("data/raw/airplane_models.csv", header=None)
airport_coordinates = pd.read_csv("data/raw/airport_coordinates.csv", header=None)
country_codes = pd.read_csv("data/raw/country_codes.csv", header=None)

# Step 1: Clean airline_data
# Set proper column names for airline data
airline_columns = ["ID", "Name", "Alias", "IATA", "ICAO", "Callsign", "Country", "Active"]
airline_data.columns = airline_columns

# Replace \N with NaN
airline_data.replace("\\N", pd.NA, inplace=True)

# Step 2: Clean airplane_models
# Set column names
airplane_columns = ["Model Name", "Code 1", "Code 2"]
airplane_models.columns = airplane_columns

# Step 3: Clean airport_coordinates
# Set column names
airport_columns = ["ID", "Name", "City", "Country", "IATA", "ICAO", "Latitude", "Longitude", "Altitude",
                   "Timezone", "DST", "Timezone Database", "Type", "Source"]
airport_coordinates.columns = airport_columns

# Replace \N with NaN
airport_coordinates.replace("\\N", pd.NA, inplace=True)

# Step 4: Clean country_codes
# Set column names
country_columns = ["Country", "ISO2", "ISO3"]
country_codes.columns = country_columns

# Replace empty strings with NaN
country_codes.replace("", pd.NA, inplace=True)

# Step 5: Save cleaned files (optional)
airline_data.to_csv("data/processed/clean_airline_data.csv", index=False)
airplane_models.to_csv("data/processed/clean_airplane_models.csv", index=False)
airport_coordinates.to_csv("data/processed/clean_airport_coordinates.csv", index=False)
country_codes.to_csv("data/processed/clean_country_codes.csv", index=False)

print("Data cleaning completed and saved to new files.")
