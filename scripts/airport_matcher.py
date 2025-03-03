import pandas as pd
import itertools
import math
import multiprocessing as mp
from tqdm import tqdm  # For the progress bar

# Load airport coordinates
airport_coordinates = pd.read_csv("/home/luisvinatea/Dev/Repos/airplanes/data/processed/clean_airport_coordinates.csv")

# Filter out airports without valid coordinates
airport_coordinates = airport_coordinates.dropna(subset=["Latitude", "Longitude"])

# Function to calculate Haversine distance
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # Radius of the Earth in kilometers
    # Convert degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

# Pre-filter based on latitude/longitude difference to save time on extreme cases
def pre_filter_distance(airport1, airport2, min_lat_diff=0.1, max_lat_diff=60, max_lon_diff=60):
    # Latitude difference should be less than `max_lat_diff`
    lat_diff = abs(airport1['Latitude'] - airport2['Latitude'])
    lon_diff = abs(airport1['Longitude'] - airport2['Longitude'])

    if lat_diff > max_lat_diff or lon_diff > max_lon_diff:
        return False
    if lat_diff < min_lat_diff and lon_diff < min_lat_diff:
        return False
    return True

# Function to process airport pairs and calculate distances in parallel
def calculate_distance_pair(pair):
    _, airport1 = pair[0]  # Extract row from tuple (index, row)
    _, airport2 = pair[1]  # Extract row from tuple (index, row)
    if pre_filter_distance(airport1, airport2):
        distance = haversine(airport1["Latitude"], airport1["Longitude"], airport2["Latitude"], airport2["Longitude"])
        return {
            "Origin": airport1["Name"],
            "Origin IATA": airport1["IATA"],
            "Destination": airport2["Name"],
            "Destination IATA": airport2["IATA"],
            "Distance (km)": distance
        }
    return None

# Generate all possible airport pairs
airport_pairs = list(itertools.combinations(airport_coordinates.iterrows(), 2))

# Set distance thresholds
min_distance = 300  # Minimum distance in kilometers (approx Boston Logan to NYC LaGuardia)
max_distance = 15000  # Maximum distance in kilometers

# Initialize tqdm progress bar
with mp.Pool(mp.cpu_count()) as pool:
    results = list(tqdm(pool.imap(calculate_distance_pair, airport_pairs), total=len(airport_pairs)))

# Filter out None results and those that do not meet the distance criteria
filtered_results = [result for result in results if result and min_distance <= result["Distance (km)"] <= max_distance]

# Convert to DataFrame
routes_df = pd.DataFrame(filtered_results)

# Save the filtered routes to a CSV
routes_df.to_csv("/home/luisvinatea/Dev/Repos/airplanes/notebooks/datasets/optimized_filtered_airport_routes.csv", index=False)

print("Optimized filtered routes have been saved to 'optimized_filtered_airport_routes.csv'.")
