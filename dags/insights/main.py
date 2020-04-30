import os

from gcs import bucket_functions

from utils.config import load_name_config
from utils.file_utils import COMPOSER_DATA_FOLDER
from utils.secret_manager import access_secret

from google.cloud import storage

import drawSvg as draw
import json
import hashlib
import base64

DISPLAY_INSIGHTS_THRESH = 25


def get_fsa_to_name():
    with open(os.path.join(get_image_card_path(),'fsa_name.json')) as f:
        data = json.load(f)
    return data

def get_image_card_path():
    if os.path.exists(COMPOSER_DATA_FOLDER):
        return os.path.join(COMPOSER_DATA_FOLDER, 'insights')
    return 'insights'

class Style(draw.DrawingBasicElement):
    TAG_NAME = 'style'
    hasContent = True
    def __init__(self, text, **kwargs):
        super().__init__(**kwargs)
        self.styleContent = text

    def writeContent(self, idGen, isDuplicate, outputFile, dryRun):
        if dryRun:
            return
        outputFile.write(self.styleContent)


def make_image_card(FSA, potential, vulnerable, high_risk, basic_total, need, need_total, self_iso, self_iso_total):
    display = True
    factor = 0
    
    if (need == None or need_total < DISPLAY_INSIGHTS_THRESH) and (self_iso == None or self_iso_total < DISPLAY_INSIGHTS_THRESH):
        display = False
        factor = 110

    translations = get_fsa_to_name()
    try:
        region = translations[FSA]
    except KeyError:
        return None

    image_card_path = get_image_card_path()

    d = draw.Drawing(600, 575-factor, origin=(0, 0))
    d.append(Style("@import url('https://fonts.googleapis.com/css?family=DM+Sans:400,400i,700,700i');"))
    d.append(Style("<![CDATA[svg text{stroke:none}]]>"))
    d.append(draw.Rectangle(0, 0, 600, 575-factor, fill='#FFFFFF'))

    d.append(draw.Text("My neighbourhood in " + region, 25, 300, 525-factor, center=True, fill="#000000", font_family="DM Sans"))

    d.append(draw.Image(25, 275-factor, 166.66667, 200,path=os.path.join(image_card_path, "left.png"), embed=True,opacity=0.38))
    d.append(draw.Image(216.6667, 275-factor, 166.66667, 200,path=os.path.join(image_card_path, "middle.png"), embed=True,opacity=0.38))
    d.append(draw.Image(408.33333, 275-factor, 166.66667, 200,path=os.path.join(image_card_path, "right.png"), embed=True,opacity=0.38))

    d.append(draw.Text("POTENTIAL", 18, 107, 450-factor, center=True, fill="#4285F4", font_weight="bold", font_family="DM Sans"))
    d.append(draw.Text("CASES", 18, 107, 425-factor, center=True, fill="#4285F4", font_weight="bold", font_family="DM Sans"))
    d.append(draw.Text("VULNERABLE", 18, 300, 450-factor, center=True, fill="#4285F4", font_weight="bold", font_family="DM Sans"))
    d.append(draw.Text("INDIVIDUALS", 18, 300, 425-factor, center=True, fill="#4285F4", font_weight="bold", font_family="DM Sans"))
    d.append(draw.Text("HIGH RISK", 18, 493, 450-factor, center=True, fill="#4285F4", font_weight="bold", font_family="DM Sans"))
    d.append(draw.Text("POTENTIAL CASES", 18, 493, 425-factor, center=True, fill="#4285F4", font_weight="bold", font_family="DM Sans"))

    d.append(
        draw.Text(str(int(potential * 100 / basic_total)) + "%", 50, 107, 375-factor, center=True, fill="#000000", font_weight="bold", font_family="DM Sans"))
    d.append(
        draw.Text(str(int(vulnerable * 100 / basic_total)) + "%", 50, 300, 375-factor, center=True, fill="#000000", font_weight="bold", font_family="DM Sans"))
    d.append(
        draw.Text(str(int(high_risk * 100 / basic_total)) + "%", 50, 493, 375-factor, center=True, fill="#000000", font_weight="bold", font_family="DM Sans"))

    d.append(draw.Text("Total Reports: " + str(basic_total), 10, 107, 305-factor, center=True, fill="#000000", font_family="DM Sans"))
    d.append(draw.Text("Total Reports: " + str(basic_total), 10, 300, 305-factor, center=True, fill="#000000", font_family="DM Sans"))
    d.append(draw.Text("Total Reports: " + str(basic_total), 10, 493, 305-factor, center=True, fill="#000000", font_family="DM Sans"))

    if display == True:
        d.append(draw.Image(25, 123, 265, 130,path=os.path.join(image_card_path, "bot_left.png"), embed=True,opacity=0.38))
        d.append(draw.Text("GREATEST NEED", 15, 155, 240, center=True, fill="#4285F4", font_weight="bold", font_family="DM Sans"))

        if need is not None and need_total >= DISPLAY_INSIGHTS_THRESH:
            if need == "financialSupport":
                d.append(draw.Text("Financial", 20, 125, 200, center=True, fill="#000000", font_weight="bold", font_family="DM Sans"))
                d.append(draw.Text("Support", 20, 125, 170, center=True, fill="#000000", font_weight="bold", font_family="DM Sans"))
                d.append(draw.Image(175, 150, 70, 70,path=os.path.join(image_card_path, "Money Icon.png"), embed=True))
            elif need == "emotionalSupport":
                d.append(draw.Text("Emotional", 20, 125, 200, center=True, fill="#000000", font_weight="bold", font_family="DM Sans"))
                d.append(draw.Text("Support", 20, 125, 170, center=True, fill="#000000", font_weight="bold", font_family="DM Sans"))
                d.append(draw.Image(180, 150, 70, 70,path=os.path.join(image_card_path, "Heart Icon.png"),embed=True))
            elif need == "medication":
                d.append(draw.Text("Medication", 20, 125, 185, center=True, fill="#000000", font_weight="bold", font_family="DM Sans"))
                d.append(draw.Image(190, 150, 70, 70,path=os.path.join(image_card_path, "Health Icon.png")))
            elif need == "food":
                d.append(draw.Text("Food/", 20, 125, 210, center=True, fill="#000000", font_weight="bold", font_family="DM Sans"))
                d.append(draw.Text("Necessary", 20, 125, 185, center=True, fill="#000000", font_weight="bold", font_family="DM Sans"))
                d.append(draw.Text("Resources", 20, 125, 160, center=True, fill="#000000", font_weight="bold", font_family="DM Sans"))
                d.append(draw.Image(190, 150, 70, 70,path=os.path.join(image_card_path, "Food Icon.png", embed=True)))
        else:
            d.append(draw.Text("Not Enough Responses",20,150,200,center=True,fill="#000000",font_weight="bold",font_family="DM Sans"))
            d.append(draw.Text("Keep Sharing!",20,150,170,center=True,fill="#000000",font_weight="bold",font_family="DM Sans")) 
          
        d.append(draw.Image(317, 123, 259, 130,path=os.path.join(image_card_path, "bot_right.png"), embed=True,opacity=0.38))
        d.append( draw.Text("INDIVIDUALS SELF ISOLATING", 15, 450, 240, center=True, fill="#4285F4", font_weight="bold",font_family="DM Sans"))
        
        if self_iso is not None and self_iso_total>=DISPLAY_INSIGHTS_THRESH:         
            d.append(
                draw.Text(str(int(self_iso * 100 / self_iso_total)) + "%", 40, 421, 190, center=True, fill="#000000", font_weight="bold", font_family="DM Sans"))
            d.append(draw.Image(460, 150, 70, 70,
                                path=("House Icon.png"),
                                embed=True))
        else:
            d.append(draw.Text("Not Enough Responses",20,450,200,center=True,fill="#000000",font_weight="bold",font_family="DM Sans"))
            d.append(draw.Text("Keep Sharing!",20,450,170,center=True,fill="#000000",font_weight="bold",font_family="DM Sans"))                      
    else:
        d.append(draw.Text("We need more submissions for more insights. Keep sharing!", 17, 300, 140, center=True, fill="#000000",font_family="DM Sans"))

    d.append(draw.Text("FLATTEN.CA", 40, 300, 60, center=True, fill="#000000", font_family="DM Sans",text_decoration="underline", font_weight = "bold"))
    d.append(draw.Text("Produced by", 20, 300, 100, center=True, fill="#000000", font_family="DM Sans"))

    d.setPixelScale(2)  # Set number of pixels per geometry unit

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

    image_hash_secret = access_secret(config['image_hash_secret'])

    for fsa, fsa_data in data['postcode'].items():
        svgText = make_image_card(
            fsa,
            fsa_data['pot'],
            fsa_data['risk'],
            fsa_data['both'],
            fsa_data['basic_total'],
            fsa_data['greatest_need'],
            fsa_data['greatest_need_total'],
            fsa_data['self_iso'],
            fsa_data['self_iso_total'],
        )

        # file name is the base 64 e
        fsa_hashed = base64.standard_b64encode(
            hashlib.pbkdf2_hmac(
                config['image_hash_algorithm'],
                fsa.encode(),
                image_hash_secret,
                int(config['image_hash_iterations'])
            )
        ).decode()
        print(fsa_hashed)

        if svgText is not None:
            bucket_functions.upload_blob(
                bucket,
                svgText,
                os.path.join(config['upload_path'], fsa_hashed + config['upload_file_ext']),
                content_type='image/svg+xml'
            )


if __name__ == "__main__":
    run_service()
