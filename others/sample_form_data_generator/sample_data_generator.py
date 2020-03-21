# This script creates a sample dataset mapping postal code FSA to a random number of pending testing cases

import json
import random
import time

MAX = {"pot": 200, "risk": 1000}

generated_data = {}

with open('../postal_code_boundries/v2/postal_code_boundaries.json') as file:
    data = json.load(file)

    for key in data.keys():
        # No data for 50%
        if random.random() < 0.8:
            continue

        # No potential for 20%
        risk = random.randint(0, MAX["risk"])
        pot = 0

        if random.random() < 0.8:
            pot = random.randint(0, MAX["pot"])

        generated_data[key] = {"pot": pot, "risk": risk}

full_data = {"time": time.time(), "max": MAX, "fsa": generated_data}

with open('form_data.js', 'w') as file:
    output_string = json.dumps(full_data)
    file.write("form_data = '" + output_string + "';")
