import os

from gcs import bucket_functions

from utils.config import load_name_config
from utils.file_utils import COMPOSER_DATA_FOLDER

from google.cloud import storage

import drawSvg as draw
import json


def get_fsa_to_name():
    with open(os.path.join(get_image_card_path(),'fsa_name.json')) as f:
        data = json.load(f)
    return data

def get_image_card_path():
    if os.path.exists(COMPOSER_DATA_FOLDER):
        return os.path.join(COMPOSER_DATA_FOLDER, 'insights')
    return 'insights'

def make_image_card(FSA, potential, vulnerable, high_risk, potential_total, vulnerable_total,high_risk_total,self_iso_total,need, self_iso):
    translations = get_fsa_to_name()
    try:
        region = translations[FSA]
    except KeyError:
        return None

    image_card_path = get_image_card_path()

    d = draw.Drawing(600, 600, origin=(0, 0))
    d.append(draw.Rectangle(0, 0, 600, 600, fill='#FFFFFF'))

    d.append(draw.Text("My neighbourhood in " + region, 20, 300, 550, center=True, fill="#000000"))

    d.append(draw.Rectangle(25, 300, 166.66667, 200, rx=10, ry=10, fill="#F5F3F2"))
    d.append(draw.Rectangle(216.6667, 300, 166.66667, 200, rx=10, ry=10, fill="#F5F3F2"))
    d.append(draw.Rectangle(408.3333, 300, 166.66667, 200, rx=10, ry=10, fill="#F5F3F2"))

    d.append(draw.Text("POTENTIAL CASES", 10, 107, 475, center=True, fill="#000000", font_weight="bold"))
    d.append(draw.Text("REPORTED", 10, 107, 460, center=True, fill="#000000", font_weight="bold"))
    d.append(draw.Text("VULNERABLE INDIVIDUALS", 10, 300, 475, center=True, fill="#000000", font_weight="bold"))
    d.append(draw.Text("REPORTED", 10, 300, 460, center=True, fill="#000000", font_weight="bold"))
    d.append(draw.Text("HIGH RISK POTENTIAL", 10, 493, 475, center=True, fill="#000000", font_weight="bold"))
    d.append(draw.Text("CASES REPORTED", 10, 493, 460, center=True, fill="#000000", font_weight="bold"))

    d.append(
        draw.Text(str(int(potential * 100 / potential_total)) + "%", 50, 107, 400, center=True, fill="#000000", font_weight="bold"))
    d.append(
        draw.Text(str(int(vulnerable * 100 / vulnerable_total)) + "%", 50, 300, 400, center=True, fill="#000000", font_weight="bold"))
    d.append(
        draw.Text(str(int(high_risk * 100 / high_risk_total)) + "%", 50, 493, 400, center=True, fill="#000000", font_weight="bold"))

    d.append(draw.Text("Total Reports: " + str(potential_total), 10, 107, 330, center=True, fill="#000000"))
    d.append(draw.Text("Total Reports: " + str(vulnerable_total), 10, 300, 330, center=True, fill="#000000"))
    d.append(draw.Text("Total Reports: " + str(high_risk_total), 10, 493, 330, center=True, fill="#000000"))

    d.append(draw.Line(25, 275, 575, 275, stroke="#F5F3F2", stroke_width=2, fill='none'))

    d.append(
        draw.Text("GREATEST NEED IN YOUR COMMUNITY", 10, 150, 245, center=True, fill="#000000", font_weight="bold"))
    if need == "financialSupport":
        d.append(draw.Text("Financial", 20, 100, 200, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Text("Support", 20, 100, 170, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Image(175, 150, 70, 70,
                            path=os.path.join(image_card_path, "Money Icon.png"),
                            embed=True))
    elif need == "emotionalSupport":
        d.append(draw.Text("Emotional", 20, 100, 200, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Text("Support", 20, 100, 170, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Image(180, 150, 70, 70,
                            path=os.path.join(image_card_path, "Heart Icon.png"),
                            embed=True))
    elif need == "medication":
        d.append(draw.Text("Medication", 20, 100, 185, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Image(190, 150, 70, 70,
                            path=os.path.join(image_card_path, "Health Icon.png")))
    elif need == "food":
        d.append(draw.Text("Food/", 20, 100, 215, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Text("Necessary", 20, 100, 185, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Text("Resources", 20, 100, 155, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Image(190, 150, 70, 70,
                            path=os.path.join(image_card_path, "Food Icon.png"),
                            embed=True))
    else:
        #print(f"need: {need}")
        d.append(draw.Text(need, 20, 100, 185, center=True, fill="#000000", font_weight="bold"))

    
    d.append(draw.Text("INDIVIDUALS IN SELF ISOLATION", 10, 450, 245, center=True, fill="#000000", font_weight="bold"))
    if self_iso is not None and self_iso_total>=25:
        d.append(
            draw.Text(str(int(self_iso * 100 / self_iso_total)) + "%", 40, 410, 190, center=True, fill="#000000", font_weight="bold"))
        d.append(draw.Image(470, 150, 70, 70,
                            path=os.path.join(image_card_path, "House Icon.png"),
                            embed=True))
    else:
        d.append(draw.Text("Not Enough",20,450,200,center=True,fill="#000000",font_weight="bold"))
        d.append(draw.Text("Responses Yet!",20,450,170,center=True,fill="#000000",font_weight="bold"))

    d.append(draw.Line(25, 125, 575, 125, stroke="#F5F3F2", stroke_width=2, fill='none'))
    d.append(draw.Image(100, 0, 400, 70,
                        path=os.path.join(image_card_path, "flatten.png"),
                        embed=True))
    d.append(draw.Text("Produced by", 20, 300, 100, center=True, fill="#000000"))

    d.setPixelScale(2)  # Set number of pixels per geometry unit

    d.saveSvg('hll.svg')
    return d.asSvg()


def run_service():
    config = load_name_config("insights_card")
    data_config = load_name_config("insights_data")

    data = bucket_functions.get_json(
        data_config['UPLOAD_BUCKET'],
        data_config['UPLOAD_FILE']
    )

    storage_client = storage.Client()
    bucket = storage_client.bucket(config['image_bucket'])

    for fsa, fsa_data in data['postcode'].items():
        svgText = make_image_card(
            fsa,
            fsa_data['pot'],
            fsa_data['risk'],
            fsa_data['both'],
            fsa_data['potential_total'],#assu
            fsa_data['vulnerable_total'],
            fsa_data['high_risk_total'],
            fsa_data['self_iso_total'],
            fsa_data['greatest_need'],
            fsa_data['self_iso']
        )
        if svgText is not None:
            bucket_functions.upload_blob(
                bucket,
                svgText,
                os.path.join(config['upload_path'], fsa + config['upload_file_ext']),
                content_type='image/svg+xml'
            )


if __name__ == "__main__":
    run_service()