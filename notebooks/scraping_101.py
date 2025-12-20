import requests
import pandas as pd
import time
from typing import List, Dict
import json

class HermesRestaurantCollector:
    """
    Collector for HERMES project: Database Creation
    Focuses on gathering restaurants in Barcelona with reviews
    """

    def __init__(self, google_api_key):
        """
        Initialize the collector with Google Places API key.

        Args:
            google_api_key (str): Google Places API key
        """
        self.api_key = google_api_key
        self.base_url = "https://maps.googleapis.com/maps/api/place"
        self.barcelona_coords = "41.3851,2.1734"

    def get_restaurants_in_area(self, location=None, radius=5000):
        """
        Get all restaurants in a specific area of Barcelona.

        Args:
            location (str): Coordinates as "lat,lng"
            radius (int): Search radius in meters (max 50000)

        Returns:
            list: List of restaurant basic data
        """
        if location is None:
            location = self.barcelona_coords

        endpoint = f"{self.base_url}/nearbysearch/json"

        params = {
            'location': location,
            'radius': radius,
            'type': 'restaurant',
            'key': self.api_key,
            'language': 'es'
        }

        all_restaurants = []

        while True:
            response = requests.get(endpoint, params=params)
            data = response.json()

            if data['status'] != 'OK':
                if data['status'] == 'ZERO_RESULTS':
                    print(f"No more results in this area")
                else:
                    print(f"Error: {data.get('status')}")
                break

            all_restaurants.extend(data['results'])

            # Check for next page
            if 'next_page_token' in data:
                time.sleep(2)  # Required by Google
                params = {
                    'pagetoken': data['next_page_token'],
                    'key': self.api_key
                }
            else:
                break

        return all_restaurants

    def get_restaurant_details(self, place_id):
        """
        Get detailed information including reviews for a restaurant.

        Args:
            place_id (str): Google Place ID

        Returns:
            dict: Detailed restaurant information with reviews
        """
        endpoint = f"{self.base_url}/details/json"

        params = {
            'place_id': place_id,
            'key': self.api_key,
            'fields': 'name,rating,formatted_address,geometry,formatted_phone_number,website,price_level,types,user_ratings_total,reviews,opening_hours',
            'language': 'es'
        }

        response = requests.get(endpoint, params=params)
        data = response.json()

        if data['status'] == 'OK':
            return data['result']
        else:
            print(f"Error getting details for {place_id}: {data.get('status')}")
            return None

    def collect_barcelona_restaurants(self, num_areas=9):
        """
        Collect restaurants from multiple areas of Barcelona to get good coverage.
        Barcelona is divided into a grid to ensure we capture all restaurants.

        Args:
            num_areas (int): Number of areas to divide Barcelona (default: 9 for 3x3 grid)

        Returns:
            pd.DataFrame: All restaurants with basic info
        """

        # Define multiple search points across Barcelona
        search_points = [
            ("41.3851,2.1734", "City Center"),      # Ciutat Vella
            ("41.3874,2.1686", "Gothic Quarter"),    # Barri Gòtic
            ("41.3947,2.1771", "Eixample"),          # Eixample
            ("41.4036,2.1744", "Gràcia"),            # Gràcia
            ("41.3797,2.1894", "Poblenou"),          # Poblenou
            ("41.3609,2.1489", "Montjuïc"),          # Montjuïc
            ("41.4145,2.1527", "Sant Andreu"),       # Sant Andreu
            ("41.3716,2.1228", "Sants"),             # Sants
            ("41.4331,2.1789", "Horta"),             # Horta-Guinardó
        ]

        all_restaurants = []
        seen_place_ids = set()

        for location, neighborhood in search_points:

            restaurants = self.get_restaurants_in_area(
                location=location,
                radius=2000
            )

            # Remove duplicates
            new_restaurants = []
            for rest in restaurants:
                if rest['place_id'] not in seen_place_ids:
                    seen_place_ids.add(rest['place_id'])
                    new_restaurants.append(rest)

            all_restaurants.extend(new_restaurants)
            print(f"New restaurants found: {len(new_restaurants)}")
            print(f"Total unique restaurants: {len(all_restaurants)}")

            # Respectful delay between areas
            time.sleep(3)

        print(f"\n{'='*60}")
        print(f"Collection complete!")
        print(f"Total restaurants found: {len(all_restaurants)}")
        print(f"{'='*60}")

        return all_restaurants

    def enrich_with_details_and_reviews(self, restaurants, max_restaurants=None):
        """
        Enrich basic restaurant data with detailed info and reviews.

        Args:
            restaurants (list): List of basic restaurant data
            max_restaurants (int): Limit number of restaurants to process (for testing)

        Returns:
            list: Enriched restaurant data with reviews
        """
        if max_restaurants:
            restaurants = restaurants[:max_restaurants]

        print(f"\n{'='*60}")
        print(f"Enriching {len(restaurants)} restaurants with details and reviews")
        print(f"{'='*60}")

        enriched_data = []

        for idx, restaurant in enumerate(restaurants, 1):
            print(f"\n[{idx}/{len(restaurants)}] {restaurant.get('name', 'Unknown')}")

            details = self.get_restaurant_details(restaurant['place_id'])

            if details:
                # Merge basic info with detailed info
                enriched = {
                    'place_id': restaurant['place_id'],
                    'name': details.get('name'),
                    'address': details.get('formatted_address'),
                    'lat': details['geometry']['location']['lat'],
                    'lng': details['geometry']['location']['lng'],
                    'rating': details.get('rating'),
                    'total_reviews': details.get('user_ratings_total', 0),
                    'price_level': details.get('price_level'),
                    'phone': details.get('formatted_phone_number'),
                    'website': details.get('website'),
                    'types': ', '.join(details.get('types', [])),
                    'reviews': details.get('reviews', [])  # Max 5 reviews from Google
                }

                enriched_data.append(enriched)
                print(f"   ✓ Rating: {enriched['rating']}, Reviews: {len(enriched['reviews'])}")

            # Delay to respect API limits (important!)
            time.sleep(0.5)

        return enriched_data

    def save_to_json(self, data, filename='hermes_restaurants.json'):
        """
        Save collected data to JSON file.

        Args:
            data (list): Restaurant data
            filename (str): Output filename
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"\n✓ Data saved to {filename}")

    def create_dataframe(self, enriched_data):
        """
        Convert enriched data to pandas DataFrame for analysis.

        Args:
            enriched_data (list): Enriched restaurant data

        Returns:
            tuple: (df_restaurants, df_reviews) - Two separate DataFrames
        """
        # Main restaurants DataFrame
        restaurants = []
        all_reviews = []

        for rest in enriched_data:
            # Restaurant info
            rest_data = {k: v for k, v in rest.items() if k != 'reviews'}
            rest_data['num_reviews_collected'] = len(rest.get('reviews', []))
            restaurants.append(rest_data)

            # Reviews (separate table)
            for review in rest.get('reviews', []):
                review_data = {
                    'place_id': rest['place_id'],
                    'restaurant_name': rest['name'],
                    'author_name': review.get('author_name'),
                    'rating': review.get('rating'),
                    'text': review.get('text'),
                    'time': review.get('time'),
                    'language': review.get('language'),
                    'relative_time': review.get('relative_time_description')
                }
                all_reviews.append(review_data)

        df_restaurants = pd.DataFrame(restaurants)
        df_reviews = pd.DataFrame(all_reviews)

        return df_restaurants, df_reviews


# ============================================================
# USAGE EXAMPLE FOR HERMES
# ============================================================

def collect_hermes_database(api_key, test_mode=False):
    """
    Complete pipeline to collect restaurant database for HERMES.

    Args:
        api_key (str): Google Places API key
        test_mode (bool): If True, only process 10 restaurants for testing
    """

    # Initialize collector
    collector = HermesRestaurantCollector(api_key)

    # Step 1: Collect all restaurants in Barcelona
    print("\n🔍 STEP 1: Collecting restaurants across Barcelona...")
    restaurants = collector.collect_barcelona_restaurants()

    # Step 2: Enrich with details and reviews
    print("\n📝 STEP 2: Getting detailed info and reviews...")
    max_rest = 10 if test_mode else None
    enriched_data = collector.enrich_with_details_and_reviews(
        restaurants,
        max_restaurants=max_rest
    )

    # Step 3: Save to files
    print("\n💾 STEP 3: Saving data...")
    collector.save_to_json(enriched_data, 'hermes_restaurants_raw.json')

    # Step 4: Create structured DataFrames
    print("\n📊 STEP 4: Creating structured datasets...")
    df_restaurants, df_reviews = collector.create_dataframe(enriched_data)

    # Save to CSV
    df_restaurants.to_csv('hermes_restaurants.csv', index=False)
    df_reviews.to_csv('hermes_reviews.csv', index=False)

    # Show summary
    print("\n" + "="*60)
    print("DATABASE COLLECTION SUMMARY")
    print("="*60)
    print(f"Total restaurants: {len(df_restaurants)}")
    print(f"Total reviews collected: {len(df_reviews)}")
    print(f"Average rating: {df_restaurants['rating'].mean():.2f}")
    print(f"Restaurants with reviews: {len(df_restaurants[df_restaurants['num_reviews_collected'] > 0])}")
    print("\nTop 5 most reviewed restaurants:")
    print(df_restaurants.nlargest(5, 'total_reviews')[['name', 'rating', 'total_reviews']])

    return df_restaurants, df_reviews


# RUN THE COLLECTION
if __name__ == "__main__":
    API_KEY = "YOUR_API_KEY_HERE"

    # Start with test mode to verify everything works
    print("Running in TEST MODE (10 restaurants only)...")
    df_rest, df_rev = collect_hermes_database(API_KEY, test_mode=True)

    # Once verified, run full collection:
    # df_rest, df_rev = collect_hermes_database(API_KEY, test_mode=False)
