# src/api_integration/zomato_client.py
import requests
from requests.oauthlib import OAuth2
from ratelimit import limits, sleep_and_retry

class ZomatoAPIClient:
    def __init__(self, api_key, base_url="https://developers.zomato.com/api/v2.1"):
        self.base_url = base_url
        self.auth = OAuth2(api_key)
        self.session = requests.Session()
        
    @sleep_and_retry
    @limits(calls=100, period=60)  # Zomato's rate limit
    def get_restaurants(self, city_id, **params):
        endpoint = f"{self.base_url}/search"
        params['entity_id'] = city_id
        params['entity_type'] = "city"
        response = self.session.get(endpoint, params=params, auth=self.auth)
        response.raise_for_status()
        return self._normalize_response(response.json())
    
    def _normalize_response(self, data):
        # Normalize API response to consistent schema
        normalized = {
            "restaurant_id": data["restaurant"]["R"]["res_id"],
            "name": data["restaurant"]["name"],
            # ... other fields
        }
        return normalized
         