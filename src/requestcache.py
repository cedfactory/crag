import requests
from typing import Optional, Dict, Any, Tuple

import json
import hashlib

class RequestCache:
    """
    A simple class to store and retrieve cached HTTP GET requests,
    and manage entries on demand.
    """

    def __init__(self):
        # Dictionary to store request results
        # Key is a tuple: (url, frozenset(params.items()), frozenset(headers.items()))
        # Value is the requests.Response
        self.cache: Dict[Tuple[str, Optional[frozenset], Optional[frozenset]], requests.Response] = {}

    def generate_dict_id(self, entry: dict) -> str:
        """
        Converts a dictionary to a canonical JSON string (sorted by keys),
        then returns an MD5 hash of that string.
        This gives you a stable ID for comparing two entries.
        """
        # Convert the dictionary to JSON with sorted keys
        canonical_str = json.dumps(entry, sort_keys=True)
        # Hash it with MD5 (or SHA256 if you want more robust hashing)
        hash_obj = hashlib.md5(canonical_str.encode("utf-8"))
        return hash_obj.hexdigest()

    def get_status(
            self,
            url: str,
            params: Optional[dict] = None,
            headers: Optional[dict] = None
    ) -> Optional[int]:
        """
        Returns the HTTP status code of a cached response if it exists,
        otherwise returns None.
        """
        key = self._make_cache_key(url, params, headers)
        response = self.cache.get(key)
        return response.status_code if response else None

    def store_request_result(
            self,
            url: str,
            response: requests.Response,
            params: Optional[dict] = None,
            headers: Optional[dict] = None
    ) -> None:
        """
        Stores the result of an HTTP request in the cache.

        :param url: The URL of the request.
        :param response: The requests.Response object containing the server's response.
        :param params: (Optional) Query parameters for the request.
        :param headers: (Optional) Headers for the request.
        """
        key = self._make_cache_key(url, params, headers)
        self.cache[key] = response

    def clear_entry(
            self,
            url: str,
            params: Optional[dict] = None,
            headers: Optional[dict] = None
    ) -> bool:
        """
        Clears a specific entry from the cache.
        Returns True if an entry was found and removed, False otherwise.
        """
        key = self._make_cache_key(url, params, headers)
        return bool(self.cache.pop(key, None))

    def _make_cache_key(
            self,
            url: str,
            params: Optional[dict],
            headers: Optional[dict]
    ) -> Tuple[str, Optional[frozenset], Optional[frozenset]]:
        """
        Creates a hashable key from the url, params, and headers.
        """
        frozen_params = frozenset(params.items()) if params else None
        frozen_headers = frozenset(headers.items()) if headers else None
        return (url, frozen_params, frozen_headers)


# Example usage:
if __name__ == "__main__":
    cache = RequestCache()

    # Make an HTTP GET request
    print("Making a request...")
    response = requests.get("https://www.example.com", params={"test": "123"})

    # Store the response in the cache
    cache.store_request_result(
        url="https://www.example.com",
        response=response,
        params={"test": "123"}
    )

    # Retrieve the status code from the cache
    status = cache.get_status("https://www.example.com", params={"test": "123"})
    print("Cached status code:", status)

    # Clear the cached entry
    cleared = cache.clear_entry("https://www.example.com", params={"test": "123"})
    print("Entry cleared?", cleared)

    # Check status again after clearing
    status_after_clear = cache.get_status("https://www.example.com", params={"test": "123"})
    print("Status after clearing:", status_after_clear)
