import requests
import csv

# Функція для отримання даних з API з пагінацією
def fetch_data_from_api(base_url):
    data = []
    next_page = 0  # Ініціалізація з валідним значенням
    while True:
        print(f"Fetching data from {base_url} with offset {next_page}")
        response = requests.get(f"{base_url}").json()
        print(f"Fetched {len(response['data'])} records")
        data.extend(response['data'])
        next_page = response['next_page']['offset']
        if not next_page:
            print("No more pages to fetch.")
            break
    return data

# Функція для запису даних у CSV
def write_csv(file_name, headers, rows):
    print(f"Writing {len(rows)} rows to {file_name}")
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)

# Збір даних із /contracts
print("Fetching contracts data...")
contracts_data = fetch_data_from_api("https://public.api.openprocurement.org/api/2.5/contracts")
print(f"Total contracts fetched: {len(contracts_data)}")

# Збір даних із /tenders для кожного contract
fact_tenders = []
dimension_region = {}
dimension_participants = {}
dimension_category = {}
dimension_items = []

for contract in contracts_data:
    tender_id = contract['tender_id']
    actual_amount = contract['value']['amount']

    try:
        print(f"Fetching tender details for TenderID: {tender_id}")
        tender_response = requests.get(f"https://public.api.openprocurement.org/api/2.5/tenders/{tender_id}").json()
        tender = tender_response['data']

        print(f"Processing TenderID: {tender_id}")

        # Дані для FactTenders
        fact_tenders.append([
            tender_id,
            tender['dateCreated'],
            tender['procuringEntity']['address']['region'],
            tender['procuringEntity']['identifier']['id'],
            tender['value']['amount'],
            actual_amount,
            tender['status'],
            len(tender.get('bids', [])),
            tender['items'][0]['classification']['description'] if tender['items'] else "N/A"
        ])

        # Дані для DimensionRegion
        region = tender['procuringEntity']['address']['region']
        if region not in dimension_region:
            dimension_region[region] = region

        # Дані для DimensionParticipants
        participant_id = tender['procuringEntity']['identifier']['id']
        if participant_id not in dimension_participants:
            dimension_participants[participant_id] = [
                participant_id,
                tender['procuringEntity']['name'],
                "Buyer",
                tender['procuringEntity']['contactPoint'].get('email', "N/A")
            ]

        # Дані для DimensionCategory та DimensionItems
        for item in tender.get('items', []):
            category = item['classification']['description']
            if category not in dimension_category:
                dimension_category[category] = category
            dimension_items.append([
                tender_id,
                item['description'],
                item['classification']['id'],
                item['classification']['description'],
                item['unit']['code'],
                item['unit']['name'],
                item['quantity']
            ])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for TenderID {tender_id}: {e}")

# Запис у CSV файли
write_csv("FactTenders.csv", [
    "TenderID", "Date", "Region", "ParticipantID", "BudgetAmount", "ActualAmount", "ContractStatus", "BidCount", "Category"
], fact_tenders)

write_csv("DimensionRegion.csv", ["Region"], [[region] for region in dimension_region.values()])

write_csv("DimensionParticipants.csv", [
    "ParticipantID", "ParticipantName", "ParticipantType", "ContactDetails"
], dimension_participants.values())

write_csv("DimensionCategory.csv", ["Category"], [[category] for category in dimension_category.values()])

write_csv("DimensionItems.csv", [
    "TenderID", "Description", "ClassificationID", "ClassificationDescription", "UnitCode", "UnitName", "Quantity"
], dimension_items)

print("CSV files have been generated.")
