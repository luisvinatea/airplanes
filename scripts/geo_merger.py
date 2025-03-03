import os
import geopandas as gpd

class FlightDataProcessor:
    def __init__(self, input_path, output_path, shapefile_path):
        self.input_path = input_path
        self.output_path = output_path
        self.shapefile_path = shapefile_path

    def load_shapefile(self, filename):
        """Load shapefile into a GeoDataFrame."""
        try:
            return gpd.read_file(os.path.join(self.shapefile_path, filename))
        except Exception as e:
            print(f"Error loading shapefile {filename}: {e}")
            return gpd.GeoDataFrame()

    def preprocess_shapefiles(self):
        # For world shapefile
        self.world_shapefile = self.world_shapefile.rename(columns={'ADMIN': 'name'})
        self.world_shapefile = self.world_shapefile[['name', 'geometry']]
        self.world_shapefile = self.world_shapefile.dropna(subset=['name', 'geometry'])
        
        # For city shapefile
        self.city_shapefile = self.city_shapefile.rename(columns={'NAME': 'name', 'ADM0NAME': 'country_name'})
        self.city_shapefile = self.city_shapefile[['name', 'country_name', 'geometry']]
        self.city_shapefile = self.city_shapefile.dropna(subset=['name', 'country_name', 'geometry'])

    def save_shapefile(self, geodataframe, filename):
        """Save GeoDataFrame to shapefile."""
        output_path = os.path.join(self.output_path, filename)
        geodataframe.to_file(output_path)
        print(f"Saved shapefile: {output_path}")

    def run(self):
        # Load shapefile data and assign to instance variables
        self.world_shapefile = self.load_shapefile("ne_110m_admin_0_countries.shp")
        self.city_shapefile = self.load_shapefile("ne_110m_populated_places.shp")

        # Preprocess shapefiles
        self.preprocess_shapefiles()

        # Save comprehensive shapefiles with only name and geometry
        self.save_shapefile(self.world_shapefile, "all_countries.shp")
        self.save_shapefile(self.city_shapefile[['name', 'geometry']], "all_cities.shp")

if __name__ == "__main__":
    shapefile_path = "/home/luisvinatea/Dev/Repos/airplanes/data/raw/shapefiles"
    input_path = "/home/luisvinatea/Dev/Repos/airplanes/data/processed"
    output_path = "/home/luisvinatea/Dev/Repos/airplanes/notebooks/datasets"

    processor = FlightDataProcessor(input_path, output_path, shapefile_path)
    processor.run()
