from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import Column, String, Integer, Float, Date
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker

from datetime import datetime
from postgresql_settings import settings
from requests import get
from requests.sessions import Session
from bs4 import BeautifulSoup
from re import search

base = declarative_base()

class Apartment(base):
    __tablename__ = "apartments"

    id = Column(Integer(), primary_key=True, autoincrement=True)
    image_url = Column(String(128))
    title = Column(String(128))
    date_added = Column(String(10))
    city = Column(String(32))
    beds = Column(Integer())
    description = Column(String(2048))
    currency = Column(String(8))
    price = Column(Float())

def get_engine(user, password, host, port, dbname):
    engine = None
    url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    try:
        if not database_exists(url):
            create_database(url)
            print(f"Database {dbname} created",) 
        
        engine = create_engine(url)

        return engine
    except:
        
        return None


eng = get_engine(
    settings["user"],
    settings["password"],
    settings["host"],
    settings["port"],
    settings["db"]
)

if not eng:
    print("Unable to create db engine")
    exit(1)

db_session = sessionmaker(eng)()

base.metadata.create_all(eng)

url = "https://www.kijiji.ca/b-apartments-condos/city-of-toronto/{}c37l1700273"

currencies = {
    "$": "USD",
    "€": "EUR" 
}

totals = []

def parse(content, url):
    global price, currency, totals

    parsed = BeautifulSoup(content, "html.parser")

    items = parsed.find_all("div", {"class": ["left-col", "info", "rental-info"]})

    for i in range(len(items) // 3):
        item = items[i * 3: i * 3 + 3]

        image = item[0].select_one("div[class='image']")

        if image.picture:
            image = image.picture.img["data-src"]
        else:
            image = image.img["src"]

        title = item[1].div.select_one("div[class='title']").a.string.strip()

        location_group = item[1].div.select_one("div[class='location']")

        date = location_group.select_one("span[class='date-posted']").string.strip()

        try:
            date = datetime.strptime(date, "%d/%m/%Y").date()
        except:
            date = datetime.now().date()

        location = location_group.select_one("span[class='']").string.strip()

        description = item[1].div.select_one("div[class='description']").text.strip()

        beds = item[2].select_one("span[class='bedrooms']")

        if (beds):
            beds = beds.text.strip()
            beds = search("\d+", beds)

            if (beds):
                beds = beds.group()

                if (beds.isnumeric()):
                    beds = int(beds)
                else:
                    beds = 0
            else:
                beds = 0
        else:
            beds = 0

        price = item[1].div.select_one("div[class='price']").text.strip()

        price = price.replace(",", "", price.count(","))

        currency = ""

        try:
            if (price[0] in currencies):
                currency = currencies[price[0]]
            
            price = float(price[1: ])
        except ValueError:
            price = 0


        item = {
            "image": image,
            "title": title,
            "date": date,
            "location": location,
            "description": description,
            "beds": beds,
            "price": price,
            "currency": currency
        }

        if item not in totals:
            totals.append(item)

    print(url, "parsed", len(totals), "objects")


session = Session()

def get_pages_count():
    total_pages = search("\/page-(?P<page>\d*)\/", get(url.format("page-99999999/")).url)

    if (total_pages):
        total_pages = int(total_pages.group("page"))

        return total_pages

    return 0

def get_content(url):
    response = get(url)

    if response.ok:
        parse(response.content, response.url)

for page in range(1, get_pages_count() + 1):
    page_url = url.format(f"page-{page}/" if page > 1 else "")

    get_content(page_url)

for apartment in totals:
    item = Apartment(
        image_url = apartment["image"],
        title = apartment["title"],
        date_added = apartment["date"].strftime("%d-%m-%Y"),
        city = apartment["location"],
        beds = apartment["beds"],
        description = apartment["description"],
        currency = apartment["currency"],
        price = apartment["price"]
    )

    db_session.add(item)

db_session.commit()