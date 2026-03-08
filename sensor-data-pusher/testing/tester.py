import requests
import json
from pathlib import Path

parent_directory = Path(__file__).parent

with open(parent_directory / "test.txt", "r") as file:
    # Runde Klammern machen eine Lazy Evaluation! Es wird ein Generator
    test_data = (json.loads(line) for line in file if line.strip())

    for entry in test_data:

        header = {"Content-Type": "application/json"}
        # Daten mit json=… schicken, sonst wird es als "Formular" formatiert
        result = requests.post(
            url="https://livedata.sebastianscharnagl.com/sensor-data-pusher/", json=entry, headers=header,
        ).status_code

        print(result)
