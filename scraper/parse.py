import pprint
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
import jsonlines
from datetime import datetime
import json

done = set()

allkeys = {
    "date",
    "elevator",
    "buildability",
    "nonEuroPrice_currency",
    "id",
    "nonEuroPrice_amount",
    "property_lotSize",
    "min_lease_time",
    "move_in",
    "gross_floor_area",
    "type",
    "water",
    "service_charge",
    "floor",
    "property_condition",
    "electricity",
    "property_subtype",
    "bath_and_wc",
    "property_energySaving",
    "listing_description",
    "office_building_category",
    "seller_id",
    "year_built",
    "view",
    "floor_area_ratio",
    "property_areaSize",
    "furnished",
    "parking_lots",
    "location",
    "attic",
    "comfort",
    "energy_certificate",
    "air_conditioning",
    "property_type",
    "building_levels",
    "ceiling_height",
    "accessible",
    "room_count",
    "view",
}

translations = {
    "akadálymentesített": "accessible",
    "parkolóhelyek_száma": "parking_lots",
    "bruttó_szintterület": "gross_floor_area",
    "költözhető": "move_in",
    "emelet": "floor",
    "min_bérleti_idő": "min_lease_time",
    "ingatlan_állapota": "property_condition",
    "min_bérelhető_terület": "min_lease_area",
    "fűtés": "heating",
    "bérleti_díj": "rent_price",
    "irodaház_kategóriája": "office_building_category",
    "belmagasság": "ceiling_height",
    "villany": "electricity",
    "víz": "water",
    "beépíthetőség": "buildability",
    "tetőtér": "attic",
    "energiatanúsítvány": "energy_certificate",
    "épület_szintjei": "building_levels",
    "kilátás": "view",
    "üzemeltetési_díj": "service_charge",
    "fürdő_és_wc": "bath_and_wc",
    "komfort": "comfort",
    "rezsiköltség": "utilities",
    "energetikai_tanúsítvány": "energy_certificate",
    "építés_éve": "year_built",
    "légkondicionáló": "air_conditioning",
    "bútorozott": "furnished",
    "szintterületi_mutató": "floor_area_ratio",
    "lift": "elevator",
}


def extract(code):
    # Regex patterns
    listing_pattern = re.search(
        r"listing = \{.*?id: (\d+),.*?type: '(\w+)'.*?rank: (\d+),.*?clusterId: (\d+),",
        code,
        re.DOTALL,
    )
    property_pattern = re.search(
        r"property = \{.*?projectId: (null|\d+),.*?type: '(\w+)',.*?subtype: '(\w+)',.*?condition: '(\w+)',.*?areaSize: (\d+),.*?lotSize: (\d+),.*?energySaving: (true|false),",
        code,
        re.DOTALL,
    )
    price_pattern = re.search(
        r"nonEuroPrice = \{ amount: (\d+), currency: \"(\w+)\" \};", code
    )
    seller_pattern = re.search(
        r"seller = \{ id: (\d+), name: \"(.*?)\".*?office: \{ id: (\d+), name: \"(.*?)\" \}\s*\};",
        code,
        re.DOTALL,
    )

    data = {}
    if listing_pattern:
        data["type"] = listing_pattern.group(2)

    if property_pattern:
        data["property_type"] = property_pattern.group(2)
        data["property_subtype"] = property_pattern.group(3)
        data["property_condition"] = property_pattern.group(4)
        data["property_areaSize"] = int(property_pattern.group(5))
        data["property_lotSize"] = int(property_pattern.group(6))
        data["property_energySaving"] = property_pattern.group(7) == "true"

    if price_pattern:
        data["nonEuroPrice_amount"] = int(price_pattern.group(1))
        data["nonEuroPrice_currency"] = price_pattern.group(2)

    if seller_pattern:
        data["seller_id"] = int(seller_pattern.group(1))

    soup = BeautifulSoup(code, "lxml")

    if soup.select_one("div#listing"):
        listing_data_raw = soup.select_one("div#listing").attrs.get(
            "data-listing", None
        )
        if listing_data_raw:
            listing_data = json.loads(listing_data_raw)
            data["type"] = listing_data.get("type", None)
            property = listing_data.get("property", None)
            data["property_type"] = property.get("type", None)
            data["property_subtype"] = property.get("subtype", None)
            data["room_count"] = property.get("roomCount", None)
            data["view"] = property.get("view", None)

            data["property_condition"] = property.get("condition", None)
            data["property_areaSize"] = property.get("areaSize", None)
            data["property_lotSize"] = property.get("lotSize", None)
            data["listing_description"] = listing_data.get("description", None)
            data["nonEuroPrice_amount"] = listing_data["prices"][0]["amount"]
            if data["nonEuroPrice_amount"] is not None and len(data["nonEuroPrice_amount"]) > 2:
                data["nonEuroPrice_amount"] = data["nonEuroPrice_amount"][:-2]
            data["nonEuroPrice_currency"] = listing_data["prices"][0]["currency"]

    data_layer = None
    if "window['dataLayer']=[" in code:
        data_layer = code.split("window['dataLayer']=[")[1].split("];")[0]
        data_layer = json.loads(data_layer)
    if "dataLayer.push(" in code:
        data_layer = code.split("dataLayer.push(")[1].split(");")[0]
        data_layer = json.loads(data_layer)
    if data_layer:
        data["property_areaSize"] = data_layer['area']
        data["location"] = data_layer['city']+', '+data_layer['county']
        data["property_condition"] = data_layer['conditionType']
        data["type"] = data_layer['listingType']
        data["nonEuroPrice_amount"] = data_layer['price']
        data["nonEuroPrice_currency"] = "HUF"
        data["property_type"] = data_layer['propertyType']
        data["property_subtype"] = data_layer['propertySubType']


    table = soup.select_one("table")
    if table is not None:
        for tr in table.select("tr"):
            key, value = [td.text.strip() for td in tr.select("td")]
            key = key.lower().replace(" ", "_").replace(".", "")
            if key in translations:
                key = translations[key]
            data[key] = value

    location_title = soup.select_one("span.card-title")
    if location_title is not None:
        data["location"] = location_title.text.strip()
    description_p = soup.select_one("p#listing-description")
    if description_p is not None:
        data["listing_description"] = description_p.text.strip()

    return data


with jsonlines.open("data.jsonl") as fr, jsonlines.open(
    "extracted_data_raw.jsonl", "w"
) as fw:
    for d in tqdm(fr):
        headers = {
            l.split(":")[0].lower(): ":".join(l.split(":")[1:]).strip()
            for l in d["content"].split("\r\n\r\n")[1].split("\r\n")[1:]
        }
        html = d["content"].split("\r\n\r\n")[2]
        extracted_data = extract(html)
        if "date" in headers:
            dt = datetime.strptime(headers["date"], "%a, %d %b %Y %H:%M:%S %Z")
            formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")
            extracted_data["date"] = formatted_date
        else:
            extracted_data["date"] = ""

        match = re.search(r"ingatlan\.com/(\d+)", d["url"])
        if match:
            extracted_data["id"] = int(match.group(1))
        else:
            print(f'error: {d["url"]}')
            continue

        if extracted_data["id"] in done:
            continue
        done.add(extracted_data["id"])
        for key in allkeys:
            if key not in extracted_data:
                extracted_data[key] = None
        # sort the keys
        extracted_data = {k: extracted_data[k] for k in sorted(allkeys)}
        fw.write(extracted_data)

print(allkeys)
