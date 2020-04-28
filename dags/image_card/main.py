import os
from gcs import bucket_functions
from utils.config import load_name_config
import drawSvg as draw
import json


def get_fsa_to_name():
    with open('fsa_name.json') as f:
        data = json.load(f)
    return data


def make_image_card(FSA, potential, vulnerable, high_risk, total, need, self_iso):
    translations = get_fsa_to_name()
    region = translations[FSA]

    d = draw.Drawing(600, 500, origin=(0, 0))
    d.append(draw.Rectangle(0, 0, 600, 500, fill='#fbfbfb'))

    d.append(draw.Text("My neighbourhood in " + region, 20, 300, 450, center=True, fill="#000000"))

    d.append(draw.Rectangle(25, 200, 166.66667, 200, fill="#dddddd"))
    d.append(draw.Rectangle(216.6667, 200, 166.66667, 200, fill="#dddddd"))
    d.append(draw.Rectangle(408.3333, 200, 166.66667, 200, fill="#dddddd"))

    d.append(draw.Text("Potential Cases", 12, 107, 375, center=True, fill="#000000", font_weight="bold"))
    d.append(draw.Text("Reported", 12, 107, 360, center=True, fill="#000000", font_weight="bold"))
    d.append(draw.Text("Vulnerable Individuals", 12, 300, 375, center=True, fill="#000000", font_weight="bold"))
    d.append(draw.Text("Reported", 12, 300, 360, center=True, fill="#000000", font_weight="bold"))
    d.append(draw.Text("High Risk Potential", 12, 493, 375, center=True, fill="#000000", font_weight="bold"))
    d.append(draw.Text("Cases Reported", 12, 493, 360, center=True, fill="#000000", font_weight="bold"))

    d.append(draw.Text(str(potential * 100 // total) + "%", 50, 107, 300, center=True, fill="#000000"))
    d.append(draw.Text(str(vulnerable * 100 // total) + "%", 50, 300, 300, center=True, fill="#000000"))
    d.append(draw.Text(str(high_risk * 100 // total) + "%", 50, 493, 300, center=True, fill="#000000"))

    d.append(draw.Text("Total Reports: " + str(total), 10, 107, 230, center=True, fill="#000000"))
    d.append(draw.Text("Total Reports: " + str(total), 10, 300, 230, center=True, fill="#000000"))
    d.append(draw.Text("Total Reports: " + str(total), 10, 493, 230, center=True, fill="#000000"))

    d.append(draw.Line(25, 175, 575, 175, stroke="#dddddd", stroke_width=2, fill='none'))

    d.append(
        draw.Text("Greatest Need In Your Community", 12, 150, 145, center=True, fill="#000000", font_weight="bold"))
    if need == "financialSupport":
        d.append(draw.Text("Financial", 25, 100, 85, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Text("Support", 25, 100, 55, center=True, fill="#000000", font_weight="bold"))

        d.append(draw.Image(180, 30, 80, 80, path="museum.png"))
    elif need == "emotionalSupport":
        d.append(draw.Text("Emotional", 25, 100, 85, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Text("Support", 25, 100, 55, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Image(180, 40, 60, 60, path="heart.png"))
    elif need == "medication":
        d.append(draw.Text("Medication", 25, 110, 75, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Image(190, 30, 80, 80, path="drug.png"))
    elif need == "food":
        d.append(draw.Text("Food/", 25, 110, 105, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Text("Necessary", 25, 110, 75, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Text("Resources", 25, 110, 45, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Image(190, 40, 60, 60, path="food.png"))
    else:
        d.append(draw.Text(need, 25, 110, 75, center=True, fill="#000000", font_weight="bold"))

    d.append(draw.Text("Individuals Self Isolating", 12, 450, 145, center=True, fill="#000000", font_weight="bold"))
    d.append(draw.Text(str(self_iso * 100 // total) + "%", 50, 405, 75, center=True, fill="#000000"))
    d.append(draw.Image(475, 30, 80, 80, path="home.png"))

    d.setPixelScale(2)  # Set number of pixels per geometry unit

    return d.asSvg()


def run_service():
    config = load_name_config("image_card")

    data = bucket_functions.download_blob(config['data_download_bucket'], config['fownload_file'])

    for fsa, fsa_data in data.items():
        svgText = make_image_card(
            fsa,
            fsa_data['pot'],
            fsa_data['risk'],
            fsa_data['both'],
            fsa_data['total'],
            fsa_data['greatst_need'],
            fsa_data['self_iso']
        )
        bucket_functions.upload_blob(
            config['data_upload_bucket'],
            svgText,
            os.path.join(config['upload_path'], fsa+config['upload_file_ext'])
        )

if __name__ == "__main__":
    run_service()
