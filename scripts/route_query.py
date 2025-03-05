import pandas as pd
import os
import logging
from data_handler import DataHandler


class RouteFinder(DataHandler):
    def __init__(self, input_path="merged/itinerary.csv",
                 output_dir="ready",
                 start_airport="FLN",
                 end_airport="LIM"):
        super().__init__(
            input_path=input_path,
            output_path=os.path.join(
                output_dir, "direct_flights.csv"))
        self.output_dir = os.path.join(self.data_dir, output_dir)
        self.start_airport = start_airport
        self.end_airport = end_airport
        self.routes = []  # List of (departure, arrival) tuples
        self.itineraries = None

    def load_data(self):
        """Load itineraries and convert Route to tuples."""
        full_path = os.path.join(self.data_dir, self.input_path)
        if not os.path.exists(full_path):
            logging.error(f"Itineraries file not found at {full_path}")
            self.df = None
            return

        self.itineraries = pd.read_csv(full_path)
        # Convert Route column to tuples (e.g., "FLN_to_LIM" -> ("FLN", "LIM"))
        self.routes = [tuple(
            route.split('_to_')
            )
            for route in self.itineraries['Route']]
        self.itineraries['Route_Tuple'] = self.routes
        logging.info(f"Loaded {len(self.itineraries)} routes from {full_path}")
        self.df = self.itineraries

    def process_data(self):
        """Find direct, 1-stop, and 2-stop routes."""
        if self.itineraries is None:
            logging.warning("No itineraries loaded!")
            return

        # Direct flights
        direct_routes = self.itineraries[
            (self.itineraries['Departure-IATA'] == self.start_airport) &
            (self.itineraries['Arrival-IATA'] == self.end_airport)
        ]

        # 1-stop flights: Find routes from start to X and X to end
        start_routes = self.itineraries[
            self.itineraries['Departure-IATA'] == self.start_airport]
        end_routes = self.itineraries[
            self.itineraries['Arrival-IATA'] == self.end_airport]
        one_stop_routes = start_routes.merge(
            end_routes,
            left_on='Arrival-IATA',
            right_on='Departure-IATA',
            how='inner',
            suffixes=('_leg1', '_leg2')
        )

        # 2-stop flights: Find start -> X -> Y -> end
        mid_routes = self.itineraries[
            (self.itineraries['Departure-IATA']
             .isin(start_routes['Arrival-IATA'])) &
            (self.itineraries['Arrival-IATA']
             .isin(end_routes['Departure-IATA']))
        ]
        two_stop_leg1 = start_routes.merge(
            mid_routes,
            left_on='Arrival-IATA',
            right_on='Departure-IATA',
            how='inner',
            suffixes=('_leg1', '_mid')
        )
        two_stop_routes = two_stop_leg1.merge(
            end_routes,
            left_on='Arrival-IATA_mid',
            right_on='Departure-IATA',
            how='inner',
            suffixes=('', '_leg3')
        )

        # Store results
        self.direct_routes = direct_routes
        self.one_stop_routes = one_stop_routes
        self.two_stop_routes = two_stop_routes

        total_routes = len(
            self.itineraries)
        found_routes = len(
            direct_routes) + len(one_stop_routes) + len(two_stop_routes)
        completeness = (
            found_routes / total_routes) * 100 if total_routes > 0 else 0
        logging.info(
            f"Analyzed {total_routes} routes. "
            f"Found: {len(direct_routes)} direct, "
            f"{len(one_stop_routes)} 1-stop, "
            f"{len(two_stop_routes)} 2-stop. "
            f"Completeness: {completeness:.2f}%")

        if found_routes == 0:
            print(
                f"No flight information found for "
                f"{self.start_airport} to {self.end_airport}.")
            print(f"Number of routes analyzed: {total_routes}")
            print(f"Database completeness score: {completeness:.2f}%")

    def save_data(self):
        """Save route combinations to separate files."""
        os.makedirs(self.output_dir, exist_ok=True)

        # Direct flights
        if not self.direct_routes.empty:
            direct_output = self.direct_routes[
                ['Airline-IATA',
                 'Airline-Name',
                 'Route']
                 ]
            direct_output.to_csv(
                os.path.join(self.output_dir, "direct_flights.csv"),
                index=False)
            logging.info(
                f"Saved {len(direct_output)} direct flights to "
                f"{self.output_dir}/direct_flights.csv")
        else:
            logging.info("No direct flights to save")

        # 1-stop flights
        if not self.one_stop_routes.empty:
            one_stop_output = self.one_stop_routes[[
                'Airline-IATA_leg1',
                'Airline-Name_leg1',
                'Route_leg1',
                'Airline-IATA_leg2',
                'Airline-Name_leg2',
                'Route_leg2'
            ]].rename(columns={
                'Airline-IATA_leg1': 'Airline-IATA_1',
                'Airline-Name_leg1': 'Airline-Name_1',
                'Route_leg1': 'Route_1',
                'Airline-IATA_leg2': 'Airline-IATA_2',
                'Airline-Name_leg2': 'Airline-Name_2',
                'Route_leg2': 'Route_2'
            })
            one_stop_output.to_csv(
                os.path.join(self.output_dir, "one_stop_flights.csv"),
                index=False)
            logging.info(
                f"Saved {len(one_stop_output)} 1-stop flights to "
                f"{self.output_dir}/one_stop_flights.csv")
        else:
            logging.info("No 1-stop flights to save")

        # 2-stop flights
        if not self.two_stop_routes.empty:
            two_stop_output = self.two_stop_routes[[
                'Airline-IATA_leg1', 'Airline-Name_leg1', 'Route_leg1',
                'Airline-IATA_mid', 'Airline-Name_mid', 'Route_mid',
                'Airline-IATA', 'Airline-Name', 'Route'
            ]].rename(columns={
                'Airline-IATA_leg1': 'Airline-IATA_1',
                'Airline-Name_leg1': 'Airline-Name_1',
                'Route_leg1': 'Route_1',
                'Airline-IATA_mid': 'Airline-IATA_2',
                'Airline-Name_mid': 'Airline-Name_2',
                'Route_mid': 'Route_2',
                'Airline-IATA': 'Airline-IATA_3',
                'Airline-Name': 'Airline-Name_3',
                'Route': 'Route_3'
            })
            two_stop_output.to_csv(
                os.path.join(self.output_dir, "two_stop_flights.csv"),
                index=False)
            logging.info(
                f"Saved {len(two_stop_output)} 2-stop flights to "
                f"{self.output_dir}/two_stop_flights.csv")
        else:
            logging.info("No 2-stop flights to save")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    # Example query: FLN to LIM
    finder = RouteFinder(start_airport="FLN", end_airport="LIM")
    finder.execute()
