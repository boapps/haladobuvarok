import json
import csv
import jsonlines


allkeys = {'date', 'elevator', 'buildability', 'nonEuroPrice_currency', 'id', 'nonEuroPrice_amount', 'property_lotSize', 'min_lease_time', 'move_in', 'gross_floor_area', 'type', 'water', 'service_charge', 'floor', 'property_condition', 'electricity', 'property_subtype', 'bath_and_wc', 'property_energySaving', 'listing_description', 'office_building_category', 'seller_id', 'year_built', 'view', 'floor_area_ratio', 'property_areaSize', 'furnished', 'parking_lots', 'location', 'attic', 'comfort', 'energy_certificate', 'air_conditioning', 'property_type', 'building_levels', 'ceiling_height', 'accessible'}

def jsonl_to_csv(jsonl_file, csv_file, keep_text=True):
    try:
        with open(jsonl_file, 'r', encoding='utf-8') as infile, open(csv_file, 'w', newline='', encoding='utf-8') as outfile:
            fieldnames = list(allkeys)
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for line in infile:
                record = json.loads(line)
                record = {k: record.get(k, None) for k in allkeys}
                if not keep_text and "listing_description" in record:
                    record.pop("listing_description", None)
                if "min_lease_area" in record:
                    record.pop("min_lease_area", None)
                writer.writerow(record)

        print(f"Conversion successful! CSV file saved as: {csv_file}")
    except Exception as e:
        print(f"Error: {e}")

def jsonl_to_jsonl(jsonl_file_1, jsonl_file_2, keep_text=True):
    try:
        with jsonlines.open(jsonl_file_1) as fr, jsonlines.open(jsonl_file_2, "w") as fw:
            for d in fr:
                if not keep_text:
                    if "min_lease_area" in d:
                        d.pop("min_lease_area", None)
                    d.pop("listing_description", None)
                fw.write(d)
        print(f"Conversion successful! JSONL file saved as: {jsonl_file_2}")
    except Exception as e:
        print(f"Error: {e}")

jsonl_to_csv("extracted_data_raw.jsonl", "extracted_data.csv", keep_text=True)
jsonl_to_csv("extracted_data_raw.jsonl", "extracted_data-no_text.csv", keep_text=False)
jsonl_to_jsonl("extracted_data_raw.jsonl", "extracted_data-no_text.jsonl", keep_text=False)
jsonl_to_jsonl("extracted_data_raw.jsonl", "extracted_data.jsonl", keep_text=True)
