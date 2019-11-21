import requests
import pandas as pd
import geopandas as gpd
from io import BytesIO
import _keys

class API_Daddy:
    def __init__(self, base_url, data_path='', query={}):
        self.base_url = base_url
        self.data_path = data_path
        self.query = query
        self.response = None

    def get_response(self):
        self.r_base_url = self.base_url
        self.r_data_path = self.data_path
        self.r_query = self.query
        self.response = requests.get(f'{self.base_url}/{self.data_path}', params=self.query)
        return self.response

    def update_response(self):
        if self.response is None:
            self.get_response()
        if (self.base_url!=self.r_base_url) or (self.data_path!=self.r_data_path) or (self.query!=self.r_query):
            self.get_response()

    def save_response(self, filename):
        self.update_response()
        with open(filename, 'w') as f:
            f.write(self.response.text)
        print(f'{filename} saved successfully.')

    def response_to_panda(self, header=0):
        self.update_response()
        self.df = pd.read_json(BytesIO(self.response.content))
        self.df.columns = self.df.iloc[header]
        return self.df

    def response_to_geopanda(self, header=0):
        self.update_response()
        self.gdf = gpd.read_file(BytesIO(self.response.content))
        return self.gdf


def main():
    census_query = {
        'get':'GIDBG,Tot_Population_ACS_13_17,Prs_Blw_Pov_Lev_ACS_13_17',
        'for':'block group:*',
        'in':['state:41','county:051'],
        'key':_keys.CENSUS_API_KEY
        }
    tiger_query = {
        'where':"STATE='41' AND COUNTY='051'",
        'outFields':'*',
        'f':'geojson'
        }

    census_daddy = API_Daddy('https://api.census.gov/data','2019/pdb/blockgroup', census_query)
    tiger_daddy = API_Daddy('https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb','Tracts_Blocks/MapServer/5/query',tiger_query)

    census_daddy.save_response('blckgrp_census.json')
    tiger_daddy.save_response('blckgrp_tiger.geojson')

    census_daddy.response_to_panda()
    tiger_daddy.response_to_geopanda()

    print(census_daddy.df.head)
    print(tiger_daddy.gdf.head)

    census_tiger_baby = tiger_daddy.gdf.merge(census_daddy.df, left_on='GEOID', right_on='GIDBG')
    census_tiger_baby.to_file('blckgrp_census_tiger_join.geojson', driver='GeoJSON')

if __name__ == '__main__':
    main()