import pandas as pd
import os
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

class FlightDataProcessor:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.airports = self.load_csv("clean_airports.csv")
        self.country_stats = self.load_csv("clean_countries.csv")
        self.routes = self.load_csv("clean_routes.csv")
        self.airlines = self.load_csv("clean_airlines.csv")

    def load_csv(self, filename):
        try:
            return pd.read_csv(os.path.join(self.input_path, filename))
        except Exception as e:
            print(f"Error loading {filename}: {e}")
            return pd.DataFrame()

    def merge_airports_data(self):
        # Merge airports with country statistics
        airports_with_country = self.airports.merge(
            self.country_stats,
            left_on='airport_country',
            right_on='country_name',
            how='left'
        )
        return airports_with_country

    def process_routes(self, clean_airports):
        # Select specific columns for source and destination airports, including country information
        source_cols = ['airport_id', 'airport_name', 'airport_latitude', 'airport_longitude', 'country_name']
        dest_cols = ['airport_id', 'airport_name', 'airport_latitude', 'airport_longitude', 'country_name']
        
        # Add suffixes to distinguish source and destination airport columns
        source_airports = clean_airports[source_cols].add_suffix('_source')
        dest_airports = clean_airports[dest_cols].add_suffix('_dest')
        
        # Merge routes with airlines
        routes_with_airlines = self.routes.merge(
            self.airlines,
            on=['airline_id', 'airline_iata'],
            how='left'
        )
        
        # Merge with source airports
        routes_with_source = routes_with_airlines.merge(
            source_airports,
            left_on='source_airport_id',
            right_on='airport_id_source',
            how='left'
        )
        
        # Merge with destination airports
        routes_with_both = routes_with_source.merge(
            dest_airports,
            left_on='destination_airport_id',
            right_on='airport_id_dest',
            how='left'
        )
        
        # Drop redundant airport_id columns from airport DataFrames
        routes_with_both = routes_with_both.drop(columns=['airport_id_source', 'airport_id_dest'])
        
        return routes_with_both

    def save_parquet(self, df, filename):
        # Save DataFrame to Parquet format
        output_file = os.path.join(self.output_path, filename)
        df.to_parquet(output_file, index=False)
        print(f"Saved: {output_file}")

    def run_pipeline(self):
        # Merge airport data and assign to clean_airports
        clean_airports = self.merge_airports_data()
        
        # Process routes using the merged airport data
        final_routes = self.process_routes(clean_airports)
        # Save final routes to Parquet
        self.save_parquet(final_routes, "direct_flights.parquet")

if __name__ == "__main__":
    input_path = "/home/luisvinatea/Dev/Repos/airplanes/data/processed"
    output_path = "/home/luisvinatea/Dev/Repos/airplanes/notebooks/datasets/parquet"
    processor = FlightDataProcessor(input_path, output_path)
    processor.run_pipeline()

    # Inspect the Parquet files
    final_routes = pd.read_parquet(os.path.join(output_path, "direct_flights.parquet"))

    print("=== Final Routes Parquet ===")
    print("Shape (rows, columns):", final_routes.shape)
    print("Columns:", final_routes.columns.tolist())
    print("First few rows:")
    print(final_routes.head())