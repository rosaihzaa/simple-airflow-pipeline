import os
import sys
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from houseELT.credentials import credentialBq
from houseELT.queryBq import queryBigquery 
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
import requests
import bs4
import json
import tqdm
import time
import random
from bs4 import BeautifulSoup
import uuid
import asyncio
import aiohttp
import nest_asyncio

sys.path.append(os.path.join(os.path.dirname(__file__), 'houseELT'))



country_name = ['italy', 'france', 'portugal', 'spain', 'switzerland', 'greece', 'malta', 'cyprus', 'Germany', 'Belgium']
headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.5",
        "referer": "https://www.google.com",
        "connection": "keep-alive"
    }
output_path = '/home/rosaihzaa/airflow/data/houses_raw.csv'
def extract_data1():

    house_link = []
    country_list = []


    async def fetch_page(session, url, country):
        async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    content = await response.text()
                    soup = BeautifulSoup(content, 'html.parser')
                    house_1 = soup.find_all('div', class_= 'item-data')
                    for house in house_1:
                            link = house.find('a', class_='link listing-title stretched-link')
                            if link:
                                link_house = link.get('href')
                                if link_house and not link_house.startswith('http'):
                                        link_house = 'https://www.properstar.com' + link_house.strip()
                                house_link.append(link_house)
                                country_list.append(country)
                else:
                    print(f'Failed {response.status}')



    async def main():
        async with aiohttp.ClientSession(headers=headers) as session:
                tasks =[]
                for country in country_name:
                    url = f'https://www.properstar.com/{country}/buy/apartment-house'
                    for hal in range(1,2):
                            url_hal = f'{url}?p={hal}'
                            tasks.append(fetch_page(session, url_hal, country))
                            await asyncio.sleep(random.uniform(1,6))
                await asyncio.gather(*tasks)

    nest_asyncio.apply()

    asyncio.run(main())

    data_house = pd.DataFrame({'Country' : country_list, 'Link' : house_link})
    return data_house

def extract_data2():
    df_house = extract_data1()
    async def fetch_detail(session, country, link, detail_houses):
        async with session.get(link) as response:
                content = await response.text()
                soup_link = BeautifulSoup(content, 'html.parser')
                house_perhal = soup_link.find_all('section', class_= 'item-intro')
                areas = soup_link.find_all('div', class_='feature-content')

                for house in house_perhal:
                    details = {'Country': country, 'Link': link}
                    name = house.find('h1')
                    name_house = name.get_text(strip=True) if name else "Unknown"
                    address = house.find('span', class_='item-info-address-inner-address')
                    address_house = address.get_text(strip=True) if address else 'Unknown'
                    price = house.find('div', class_='listing-price-main')
                    price_house = price.get_text(strip=True) if price else "Unknown"
                    details['Name'] = name_house
                    details['Address'] = address_house
                    details['Price'] = price_house
                    for area in areas:
                            key_element_area = area.find('span', class_='property-key')
                            value_element_area = area.find('span', class_='property-value')
                            if key_element_area and value_element_area:
                                key_area = key_element_area.get_text(strip=True)
                                value_area = value_element_area.get_text(strip=True)
                                details[key_area] = value_area

                    aminities = soup_link.find_all('section', class_='listing-section amenities-section')
                    facilities = []
                    for facility in aminities:
                            value_element_facility = facility.find_all('span', class_='property-value')
                            for value_facility in value_element_facility:
                                value_facilities = value_facility.get_text(strip=True)
                                facilities.append(value_facilities)
                    details['Aminities'] = facilities

                    view_houses =[]
                    views = soup_link.find_all('section', class_='listing-section views-section')
                    for view in views:
                            value_element_view = view.find_all('span', class_='property-value')
                            for value_view in value_element_view:
                                value_views = value_view.get_text(strip=True)
                                view_houses.append(value_views)
                    details['View'] = view_houses

                    detail_houses.append(details)
                await asyncio.sleep(random.uniform(1,6))

    async def houses_link():
        async with aiohttp.ClientSession(headers=headers) as session:
                tasks =[]
                detail_houses = []
                for country, link in zip(df_house['Country'], df_house['Link']):
                    tasks.append(fetch_detail(session, country, link, detail_houses))
                await asyncio.gather(*tasks)
                return detail_houses

    nest_asyncio.apply()

    data_raw = asyncio.run(houses_link())

    data = pd.DataFrame(data_raw)

    #delete unimportant columns
    data['Environmental carbon dioxide impact rating'] = data['Environmental (CO₂) impact rating']
    data['Parking lots inside'] = data['Parking lots (inside)']
    data['Parking lots outside'] = data['Parking lots (outside)']
    data.drop(['Environmental (CO₂) impact rating', 'Parking lots (inside)', 'Parking lots (outside)', 'Garages (outside)', 'Garages (inside)'], axis=1, errors='ignore', inplace=True)
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    data.to_csv(output_path, index=False)


def load_data():
    data_house = pd.read_csv(output_path)
    project_id = 'bank-marketing-project-446413'
    dataset_id = 'houses'
    table_id = 'house_airflow'
    
    client = bigquery.Client(project=project_id, credentials=credentialBq())
    table_ref = f'{project_id}.{dataset_id}.{table_id}'

    job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition='WRITE_TRUNCATE'
    )
    
    client.load_table_from_dataframe(data_house, table_ref, job_config=job_config)

def transform_data():
      query = queryBigquery()
      return query

    
    



    
    

