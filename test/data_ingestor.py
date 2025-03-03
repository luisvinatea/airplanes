import os
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, clear_output

# Helper function to update Parquet file
def update_parquet(csv_path, parquet_path):
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return

    try:
        df = pd.read_csv(csv_path)
        df.to_parquet(parquet_path, index=False)
        print(f"Parquet file successfully updated at {parquet_path}")
    except Exception as e:
        print(f"An error occurred while updating Parquet: {e}")

class FlightDataIngestor:
    def __init__(self, input_path, output_path):
        """
        Initialize paths, load source datasets, and set up UI.
        """
        self.input_path = input_path
        self.output_path = output_path

        # Define file paths for CSV files
        self.routes_csv = os.path.join(self.input_path, 'routes.csv')
        self.airlines_csv = os.path.join(self.input_path, 'airlines.csv')
        self.airports_csv = os.path.join(self.input_path, 'airports.csv')
        self.itineraries_csv = os.path.join(self.output_path, 'itineraries.csv')

        # Define file path for the Parquet file
        self.parquet_file = os.path.join(self.output_path, 'flight_metadata.parquet')

        # Load source datasets
        self.df_routes = pd.read_csv(self.routes_csv)
        self.df_airlines = pd.read_csv(self.airlines_csv)
        self.df_airports = pd.read_csv(self.airports_csv)
        self.df_itineraries = pd.read_csv(self.itineraries_csv) if os.path.exists(self.itineraries_csv) else pd.DataFrame()

        # Set up UI
        self.routes_out = widgets.Output()
        self.routes_source_input = widgets.Text(description='Source City:')
        self.routes_dest_input = widgets.Text(description='Destination City:')
        self.routes_search_button = widgets.Button(description='Search Routes')
        self.routes_search_button.on_click(self.on_routes_search)
        self.routes_ui = widgets.VBox([
            widgets.HBox([self.routes_source_input, self.routes_dest_input, self.routes_search_button]),
            widgets.HTML('<hr>')
        ])

    def on_routes_search(self, b):
        """
        Searches routes in df_routes based on source and destination, then displays editable rows.
        """
        with self.routes_out:
            clear_output()
            source = self.routes_source_input.value.strip()
            dest = self.routes_dest_input.value.strip()

            if not source or not dest:
                print("⚠️ Please enter both source and destination.")
                return

            results = self.search_routes(source, dest)
            if not results.empty:
                print(f"🔍 Found {len(results)} matching routes")
                for idx, row in results.iterrows():
                    row_widgets = self.create_editable_row(row)
                    save_btn = widgets.Button(description="Save Changes", button_style='success')
                    add_flight_btn = widgets.Button(description="Add More Flights", button_style='info')
                    container = widgets.VBox([
                        widgets.GridBox(row_widgets, layout=widgets.Layout(grid_template_columns="repeat(3, 300px)")),
                        widgets.HBox([save_btn, add_flight_btn])
                    ])
                    save_btn.on_click(lambda btn, idx=idx, w_list=row_widgets: self.save_route_changes(idx, w_list))
                    add_flight_btn.on_click(lambda btn, source=source, dest=dest: self.add_more_flights(source, dest))
                    display(container)
                    display(widgets.HTML('<hr>'))
            else:
                print("⚠️ No matching routes found. Creating a new route.")
                empty_row = self.create_empty_route()
                row_widgets = self.create_editable_row(empty_row)
                save_btn = widgets.Button(description="Create New Route", button_style='primary')
                container = widgets.VBox([
                    save_btn,
                    widgets.GridBox(row_widgets, layout=widgets.Layout(grid_template_columns="repeat(3, 300px)"))
                ])
                save_btn.on_click(lambda btn, w_list=row_widgets: self.save_route_changes(-1, w_list))
                display(container)

    def search_routes(self, source, dest):
        """
        Search the routes DataFrame for rows matching the given source and destination.
        """
        mask_source = self.df_routes['source_city'].str.contains(source, case=False, na=False)
        mask_dest = self.df_routes['destination_city'].str.contains(dest, case=False, na=False)
        return self.df_routes[mask_source & mask_dest]

    def create_empty_route(self):
        """
        Create an empty template row with the same columns as df_routes.
        """
        return pd.Series({col: '' for col in self.df_routes.columns})

    def create_editable_row(self, row):
        """
        Generate a list of Text widgets for each field in the row.
        Use IntText for integer columns (IDs, stops, etc.).
        Lock or hide ID fields to prevent manual input.
        """
        widgets_list = []
        style = {'description_width': '200px'}
        layout = widgets.Layout(width='300px')

        # Define integer columns
        integer_columns = ['airline_id', 'source_airport_id', 'destination_airport_id', 'stops']

        for col in self.df_routes.columns:
            value = row[col] if not pd.isna(row[col]) else ''
            if col in integer_columns:
                # Use IntText for integer columns but disable editing for IDs
                if col == 'stops':
                    widget_instance = widgets.IntText(
                        value=int(value) if value else 0,
                        description=col,
                        layout=layout,
                        style=style
                    )
                else:
                    widget_instance = widgets.IntText(
                        value=int(value) if value else 0,
                        description=col,
                        layout=layout,
                        style=style,
                        disabled=True  # Lock the field
                    )
            else:
                # Use Text for other columns
                widget_instance = widgets.Text(
                    value=str(value),
                    description=col,
                    layout=layout,
                    style=style
                )
            widgets_list.append(widget_instance)

        return widgets_list

    def get_or_generate_id(self, iata_code, df, id_col, iata_col):
        """
        Fetch the ID for a given IATA code or generate a new one if it doesn't exist.
        """
        if iata_code:
            match = df[df[iata_col] == iata_code]
            if not match.empty:
                return match.iloc[0][id_col]
            else:
                # Generate a new ID
                new_id = df[id_col].max() + 1 if not df.empty else 1
                return new_id
        return None

    def save_route_changes(self, index, widgets_list):
        """
        Save changes from the editable row into the df_routes DataFrame and update source CSVs.
        Automatically fetch or generate IDs for airlines and airports.
        """
        new_data = {widget.description: widget.value for widget in widgets_list}

        # Fetch or generate IDs for airlines and airports
        airline_iata = new_data.get('airline_iata', '')
        source_airport_iata = new_data.get('source_airport_iata', '')
        dest_airport_iata = new_data.get('destination_airport_iata', '')

        new_data['airline_id'] = self.get_or_generate_id(airline_iata, self.df_airlines, 'airline_id', 'airline_iata')
        new_data['source_airport_id'] = self.get_or_generate_id(source_airport_iata, self.df_airports, 'airport_id', 'airport_iata')
        new_data['destination_airport_id'] = self.get_or_generate_id(dest_airport_iata, self.df_airports, 'airport_id', 'airport_iata')

        if index == -1:  # New route
            self.df_routes = pd.concat([self.df_routes, pd.DataFrame([new_data])], ignore_index=True)
        else:  # Existing route
            for col, value in new_data.items():
                self.df_routes.at[index, col] = value

        # Save updated datasets to CSV files
        self.df_routes.to_csv(self.routes_csv, index=False)
        print("✅ Routes updated successfully!")

        # Update Parquet file
        update_parquet(self.routes_csv, self.parquet_file)

    def add_more_flights(self, source, dest):
        """
        Add a new row with the same source and destination as the found route.
        """
        with self.routes_out:
            empty_row = self.create_empty_route()
            empty_row['source_city'] = source
            empty_row['destination_city'] = dest
            row_widgets = self.create_editable_row(empty_row)
            save_btn = widgets.Button(description="Create New Flight", button_style='primary')
            container = widgets.VBox([
                widgets.GridBox([save_btn] + row_widgets, layout=widgets.Layout(grid_template_columns="repeat(3, 300px)"))
            ])
            save_btn.on_click(lambda btn, w_list=row_widgets: self.save_route_changes(-1, w_list))
            display(container)
            display(widgets.HTML('<hr>'))

    def run_ui(self):
        """
        Display the user interface.
        """
        with self.routes_out:
            display(self.routes_ui)
        display(self.routes_out)

# Example usage
if __name__ == "__main__":
    input_path = "/home/luisvinatea/Dev/Repos/airplanes/notebooks/datasets/csv"
    output_path = "/home/luisvinatea/Dev/Repos/airplanes/notebooks/datasets/parquet"

    ingestor = FlightDataIngestor(input_path, output_path)
    ingestor.run_ui()