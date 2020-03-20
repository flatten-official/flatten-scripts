# This script creates a sample dataset mapping postal code FSA to a random number of pending testing cases

import json
import random
import time

MAX_NUMBER = 1000

generated_data = {}

with open('../postal_code_boundries/v2/postal_code_boundaries.json') as file:
    data = json.load(file)

    for key in data.keys():
        if random.random() < 0.1:
            generated_data[key] = random.randint(0, MAX_NUMBER)
        else:
            generated_data[key] = 0

full_data = {"time": time.time(), "max": MAX_NUMBER, "fsa": generated_data}

with open('high_risk_SAMPLE.js', 'w') as file:
    output_string = json.dumps(full_data)
    file.write("data_high_risk_sample = '"+output_string + "';")
