import pandas as pd

# Load datasets
df_routes = pd.read_csv("/home/luisvinatea/Dev/Repos/airplanes/data/processed/clean_routes.csv")
df_airports = pd.read_csv("/home/luisvinatea/Dev/Repos/airplanes/data/processed/clean_airports.csv")
df_airlines = pd.read_csv("/home/luisvinatea/Dev/Repos/airplanes/data/processed/clean_airlines.csv")
df_planes = pd.read_csv("/home/luisvinatea/Dev/Repos/airplanes/data/processed/clean_planes.csv")
df_countries = pd.read_csv("/home/luisvinatea/Dev/Repos/airplanes/data/processed/clean_countries.csv")

# Create mapping dictionaries
airport_city_map = df_airports.set_index("airport_id")["airport_city"].to_dict()
airport_name_map = df_airports.set_index("airport_id")["airport_name"].to_dict()
airline_name_map = df_airlines.set_index("airline_id")["airline_name"].to_dict()

# Map new columns
df_routes["source_city"] = df_routes["source_airport_id"].map(airport_city_map)
df_routes["destination_city"] = df_routes["destination_airport_id"].map(airport_city_map)
df_routes["source_airport_name"] = df_routes["source_airport_id"].map(airport_name_map)
df_routes["destination_airport_name"] = df_routes["destination_airport_id"].map(airport_name_map)
df_routes["airline_name"] = df_routes["airline_id"].map(airline_name_map)

# Save updated dataset
df_routes.to_csv("/home/luisvinatea/Dev/Repos/airplanes/data/processed/clean_routes.csv", index=False)

print("✅ clean_routes.csv updated successfully with city, airport name, and airline name mappings.")

# Print dataset information
print(df_airports.dtypes)
print(df_airports.shape)
print(df_airports.head(1))

print(df_airlines.dtypes)
print(df_airlines.shape)
print(df_airlines.head(1))

print(df_planes.dtypes)
print(df_planes.shape)
print(df_planes.head(1))

print(df_countries.dtypes)
print(df_countries.shape)
print(df_countries.head(1))

print(df_routes.dtypes)
print(df_routes.shape)
print(df_routes.head(1))