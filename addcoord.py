import geocoder
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


class AddCoord:
    def __init__(self) -> None:
        self.full_addresses = None
        self.lat_lon_series = None

    def _find_duplicate_address(self, address_list: list) -> list:
        '''
        Checks if any addresses in a list of addresses are duplicated. 
        Returns duplicate addresses

        Parameters
        ----------
        address_list : list
            A list of addresses to be checked.

        Returns
        -------
        duplicate_addresses : list
            A list of duplicated addresses
        '''
        unique_addresses = set()
        duplicate_addresses = []
        for item in address_list:
            if item in unique_addresses:
                duplicate_addresses.append(item)
            unique_addresses.add(item)
        
        return duplicate_addresses
    
    def create_main_address(self, *addresses: pd.Series) -> pd.Series:
        '''
        Returns a full address if multiple addresses from different sources are passed.

        Parameters
        ----------
        *addresses : pd.Series
            Any number of address strings passed in as series objects. 
            Series objects must be equal in length.

        Returns
        -------
        full_address : pd.Series
            A series of full addresses concatenated from the various series passed.
        '''
        cleaned_addresses = [address.fillna('').str.replace(',', '').str.strip() for address in addresses]
        full_addresses = pd.concat(cleaned_addresses, axis=1).apply(lambda row: ' '.join(row), axis=1)

        # Check for duplicated addresses to alert the users if there are duplicate addresses after 
        # addresses have been created.
        duplicate_addresses = self._find_duplicate_address(full_addresses)
        if duplicate_addresses:
            print(f'After address processing, {len(duplicate_addresses)} duplicated addresses were found')

        # Update self.full_addresses attribute so that it can be accessed without any new function call.
        self.full_addresses = full_addresses

        return full_addresses
    
    def geocode(self, address: str) -> str:
        '''
        Returns latitude and longitude coordinates of addresses.

        Parameters
        ----------
        address : str
            Full address string for fetching coordinates

        Returns
        -------
        lat,lon : str
            Latitude and longitude coordinates of the passed address
        '''
        g = geocoder.arcgis(address)
        if g.ok:
            return f'{g.lat},{g.lng}'
        else:
            return None
        
    def _geocode_wrapper(self, address: str, index: dict) -> tuple:
        '''
        Returns lat,lon coordinates and corresponding indexes of the list of address passed to it.
        Utilizes the custom geocode function

        Parameters
        ----------
        address : str
            Address string
        index : dict (dict.items())
            Passed to track the index of the addresses list in the main_concurrent function
            Key-value dictionary where the key corresponds to the address and the 
            value is the index of each item in the addresses list in the main_concurrent function.

        Returns
        -------
        result : lat,lon (str)
            Latitude and longitude coordinates of the passed address
        index : dict
            Key-value dictionary where the key corresponds to the address and the 
            value is the index of each item in the addresses list in the main_concurrent function.

        '''
        lat_lon = self.geocode(address)
        if lat_lon is not None:
            return (lat_lon, index)
        else:
            return (None, index)
        
    def concurrent_geocode(self, addresses: list, index_dict: dict, num_threads: int=None) -> tuple:
        '''
        Runs the geocode_wrapper function in parallel with multiple threads.

        Parameters
        ----------
        addresses : list
            List of address strings
        index_dict : dict
            Key-value dictionary where the keys correspond to each item in the addresses 
            list and the values are the indexes of each item in the addresses list.
        num_threads : int (optional)
            Optional parameter to specify the number of threads to use for parallel processing. 
            Value cannot exceed the number of available device cores

        Returns
        -------
        (lat_lon_list, failed_indexes, successful_indexes) : tuple
        lat_lon_list : list
            List of lat,lon coordinates strings
        failed_indexes : dict
            Dictionary corresponding to key-value(index) pairs of addresses that were 
            unsuccessfully processed from the addresses list
        successful_indexes : dict
            Dictionary corresponding to key-value(index) pairs of addresses that were 
            successfully processed from the addresses list

        '''
        with ThreadPoolExecutor(num_threads) as executor:
            print(f'Getting coordinates with {executor._max_workers} threads...' )
            indexed_results = list(executor.map(self._geocode_wrapper, addresses, index_dict.items()))

            lat_lon_list = [result[0] for result in indexed_results]
            failed_indexes_list = [result[1] for result in indexed_results if result[0] is None]
            successful_index_list = [result[1] for result in indexed_results if result[0] is not None]
            failed_indexes = {k: v for k, v in failed_indexes_list}
            successful_indexes = {k: v for k, v in successful_index_list}

        return (lat_lon_list, failed_indexes, successful_indexes)
    
    def fetch_coordinates(self, addresses: list | pd.Series, num_threads: int=None) -> pd.Series:
        '''
        Returns a Series object containing corresponding latitude and longitude coordinates of 
        addresses passed to it.

        Parameters
        ----------
        addresses : list | pd.Series
            A list or series of addresses
        num_threads : int (optional; default = None)
            Optional parameter to specify the number of threads to use for parallel processing. 
            Value cannot exceed the number of available device cores

        Returns
        -------
        lat_lon_series : pd.Series
            A Series object containing corresponding latitude and longitude coordinates of 
            addresses passed to it
        '''
        # Create a dictionary of the indexes of each address in the addresses list to
        # track failures and retries 
        index_dict = {k: v for v, k in enumerate(addresses)}

        lat_lon_list, failed_indexes, successful_indexes = self.concurrent_geocode(addresses, index_dict, num_threads)

        retries = 0
        while failed_indexes and retries < 10:
            print('Retrying failed requests...')

            # Update index_dict
            index_dict = failed_indexes
            index_list = index_dict.values()

            # Retry concurrent_geocode for failed entries
            lat_lon_retries, failed_indexes, successful_indexes = self.concurrent_geocode([addresses[index] for index in index_list], index_dict, num_threads)
            
            # Update lat_lon_list
            update_list = [(x, y) for x, y in zip(lat_lon_retries, successful_indexes.values())]
            for coord, index in update_list:
                lat_lon_list[index] = coord
        
            retries += 1

        # Convert lat_lon_list to a Series object
        lat_lon_series = pd.Series(lat_lon_list)

        # Update self.lat_lon_series object
        self.lat_lon_series = lat_lon_series

        return lat_lon_series
