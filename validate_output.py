import json
from models import Item  # import from models.py

# Load JSON file
with open("theinfatuation_london_1.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Validate each item
for i, raw_item in enumerate(data):
    try:
        item = Item.model_validate(raw_item)
        print(f"✅ Item {i + 1} is valid")
    except Exception as e:
        print(f"❌ Item {i + 1} is INVALID")
        print(e)
