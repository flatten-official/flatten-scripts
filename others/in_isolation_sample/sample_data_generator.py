# This script creates a sample dataset mapping postal code FSA to a random number of pending testing cases

import json
import random
import time

MAX_NUMBER = 1000

generated_data = {}
maximum = 0

with open('../postal_code_boundries/v2/postal_code_boundaries.json') as file:
    data = json.load(file)

    for key in data.keys():
    # fill generated_data with: key -> {mild -> num_mild, severe -> num_severe}
        if random.random() < 0.1:
            num_mild = random.randint(0, MAX_NUMBER)
            num_severe = int(num_mild * 0.2)
            generated_data[key] = {"mild": num_mild, "severe": num_severe}
            maximum = max(maximum, num_mild + num_severe * 2)
        else:
            generated_data[key] = {"mild": 0, "severe": 0}

full_data = {"time": time.time(), "max": maximum, "fsa": generated_data}

with open('in_self_isolation_SAMPLE.js', 'w') as file:
    output_string = json.dumps(full_data)
    file.write("data_in_self_isolation_sample = '"+output_string + "';")
