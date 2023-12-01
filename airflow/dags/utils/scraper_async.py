import asyncio
import httpx
import json
import pandas
import re
import ssl
import time
from datetime import date
from typing import List
from bs4 import BeautifulSoup
from collections import defaultdict
from itertools import chain


def set_property_id(property_dict: dict):
    if property_dict["id"]:
        property_id = property_dict["id"]
        return property_id
    else:
        return None


def set_locality_name(property_dict: dict):
    if property_dict["property"]["location"]["locality"]:
        locality_name = property_dict["property"]["location"]["locality"]
        return locality_name
    else:
        return None


def set_postal_code(property_dict: dict):
    if property_dict["property"]["location"]["postalCode"]:
        postal_code = property_dict["property"]["location"]["postalCode"]
        return postal_code
    else:
        return None


def set_type_of_property(property_dict: dict):
    if property_dict["property"]["type"]:
        type_of_property = property_dict["property"]["type"]
        return type_of_property
    else:
        return None


def set_subtype_of_property(property_dict: dict):
    if property_dict["property"]["subtype"]:
        subtype_of_property = property_dict["property"]["subtype"]
        return subtype_of_property
    else:
        return None


def set_price(property_dict: dict):
    if property_dict["price"]["mainValue"]:
        price = property_dict["price"]["mainValue"]
        return price
    else:
        return None


def set_type_of_sale(property_dict: dict):
    if property_dict["flags"]["isPublicSale"]:
        type_of_sale = "Public Sale"
        return type_of_sale
    elif property_dict["flags"]["isNewlyBuilt"]:
        type_of_sale = "Newly Built"
        return type_of_sale
    elif property_dict["flags"]["isNotarySale"]:
        type_of_sale = "Notary Sale"
        return type_of_sale
    elif property_dict["flags"]["isLifeAnnuitySale"]:
        type_of_sale = "Life Annuity Sale"
        return type_of_sale
    elif property_dict["flags"]["isAnInteractiveSale"]:
        type_of_sale = "Interactive Sale"
        return type_of_sale
    elif property_dict["flags"]["isUnderOption"]:
        type_of_sale = "Under Option"
        return type_of_sale
    else:
        return None


def set_number_of_rooms(property_dict: dict):
    if property_dict["property"]["bedroomCount"]:
        number_of_rooms = property_dict["property"]["bedroomCount"]
        return number_of_rooms
    else:
        return None


def set_living_area(property_dict: dict):
    if property_dict["property"]["netHabitableSurface"]:
        living_area = property_dict["property"]["netHabitableSurface"]
        return living_area
    else:
        return None


def set_equipped_kitchen(property_dict: dict):
    if property_dict["property"]["kitchen"]:
        return 1
    else:
        return 0


def set_furnished(property_dict: dict):
    if property_dict["transaction"]["sale"]["isFurnished"]:
        return 1
    else:
        return 0


def set_open_fire(property_dict: dict):
    if property_dict["property"]["fireplaceExists"]:
        return 1
    else:
        return 0


def set_terrace(property_dict: dict):
    if property_dict["property"]["terraceSurface"]:
        terrace_surface = property_dict["property"]["terraceSurface"]
        return terrace_surface
    else:
        return None


def set_garden(property_dict: dict):
    if property_dict["property"]["gardenSurface"]:
        garden_area = property_dict["property"]["gardenSurface"]
        return garden_area
    else:
        return None


def set_surface_of_good(property_dict: dict):
    if property_dict["property"]["land"]:
        surface_of_good = property_dict["property"]["land"]["surface"]
        return surface_of_good
    else:
        return None


def set_number_of_facades(property_dict: dict):
    if property_dict["property"]["building"]:
        number_of_facades = property_dict["property"]["building"]["facadeCount"]
        return number_of_facades
    else:
        return None


def set_swimming_pool(property_dict: dict):
    if property_dict["property"]["hasSwimmingPool"]:
        return 1
    else:
        return 0


def set_state_of_building(property_dict: dict):
    if property_dict["property"]["building"]:
        state_of_building = property_dict["property"]["building"]["condition"]
        return state_of_building
    else:
        return None


async def fetch_urls(url, session):
    try:
        property_urls = []
        req = await session.get(url)
        soup = BeautifulSoup(req.content, "html.parser")
        pattern = "group"
        for tag in soup.find_all("a", attrs={"class": "card__title-link"}):
            if re.search(pattern, tag["aria-label"]):
                continue
            elif tag["href"] in property_urls:
                continue
            else:
                property_urls.append(tag["href"])
        with open("./dataset/urls.txt", "a") as file:
            for url in property_urls:
                file.write(str(url) + "\n")
        return property_urls
    except httpx.ConnectError as ce:
        print(f"Connection error: {ce}")
    except httpx.HTTPStatusError as hse:
        print(f"HTTP status error: {hse}")
    except ssl.SSLSyscallError as ssl_error:
        print(f"SSL error: {ssl_error}")
    except ssl.SSLWantReadError as ssl_error:
        print(f"SSL read error: {ssl_error}")
    except ssl.SSLError as ssl_error:
        print(f"SSL error: {ssl_error}")
    except Exception as e:
        print(f"An error occurred: {e}")


async def get_property_dict(url, session):
    req = await session.get(url)
    soup = BeautifulSoup(req.content, "html.parser")
    try:
        script = soup.find("script", attrs={"type": "text/javascript"})
        data = script.string
        data.strip()
        data = data[data.find("{"):data.rfind("}") + 1]
        property_dict = json.loads(data)
        return property_dict
    except:
        return None


async def get_urls(x: int = 1, y: int = 334, type: str = "house"):
    async with httpx.AsyncClient(timeout=None) as session:
        tasks = []
        for i in range(x, y):
            print(f"Page {i}")
            root: str = f"https://www.immoweb.be/en/search/{type}/for-sale?countries=BE&page={i}&orderBy=relevance"
            tasks.append(asyncio.ensure_future(fetch_urls(root, session)))
        return await asyncio.gather(*tasks)


async def get_details(urls):
    limits = httpx.Limits(max_connections=25)
    async with httpx.AsyncClient(timeout=None, limits=limits) as session:
        tasks = []
        for url in urls:
            tasks.append(asyncio.ensure_future(get_async_detail(url, session)))
        return await asyncio.gather(*tasks)


async def get_async_detail(url, session):
    property_dict = await get_property_dict(url, session)
    data = defaultdict(list)
    data["property_id"] = set_property_id(property_dict)
    print(data["property_id"])
    data["url"] = url
    data["locality_name"] = set_locality_name(property_dict)
    data["postal_code"] = set_postal_code(property_dict)
    data["type_of_property"] = set_type_of_property(property_dict)
    data["subtype_of_property"] = set_subtype_of_property(property_dict)
    data["price"] = set_price(property_dict)
    data["type_of_sale"] = set_type_of_sale(property_dict)
    data["number_of_rooms"] = set_number_of_rooms(property_dict)
    data["living_area"] = set_living_area(property_dict)
    data["equipped_kitchen"] = set_equipped_kitchen(property_dict)
    data["furnished"] = set_furnished(property_dict)
    data["open_fire"] = set_open_fire(property_dict)
    data["terrace"] = set_terrace(property_dict)
    data["garden"] = set_garden(property_dict)
    data["surface_of_good"] = set_surface_of_good(property_dict)
    data["number_of_facades"] = set_number_of_facades(property_dict)
    data["swimming_pool"] = set_swimming_pool(property_dict)
    data["state_of_building"] = set_state_of_building(property_dict)
    data["date"] = date.today()
    return data


def scraper_async(x: int = 1, y: int = 334, type: str = 'house'):
    """
    scraper function
    :param x: First page (default = 1)
    :param y: Last page (max = 334, default = max)
    :param type: Either 'house' or 'apartment' (default = 'house')
    :return: create a csv with the data
    """
    start = time.time()
    urls = asyncio.run(get_urls(x, y, type))
    detail_urls = []
    for url in chain(urls):
        for u in chain(url):
            detail_urls.append(u)
    details = asyncio.run(get_details(detail_urls))

    df = pandas.DataFrame(details)
    current_date = date.today()
    df.to_csv(f"./dataset/immo_data_{type}_{current_date}.csv", na_rep="None", index=False)
    print(f"Scraped {type} in {(time.time() - start): .2f} seconds")
