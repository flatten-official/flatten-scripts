
import json
import pyproj

CONVERT_IN_PROJ = pyproj.Proj('epsg:3347')
CONVERT_OUT_PROJ = pyproj.Proj('epsg:4326')
TRANSFORMER = pyproj.Transformer.from_proj(CONVERT_IN_PROJ, CONVERT_OUT_PROJ, always_xy=True)

def convert(filename):
    with open(filename, 'r') as file:
        input_data = json.load(file)
    output_data = input_data
    for i in range(len(output_data["features"])):
        if output_data["features"][i]['geometry']['type'] == 'Polygon':
            for j in range(len(output_data["features"][i]['geometry']['coordinates'])):
                coords = []
                for lng, lat in TRANSFORMER.itransform(output_data["features"][i]['geometry']['coordinates'][j]):
                    coords.append([round(lng, 4), round(lat, 4)])
                output_data["features"][i]['geometry']['coordinates'][j] = coords
                      
        elif output_data["features"][i]['geometry']['type'] == 'MultiPolygon':
            for j in range(len(output_data["features"][i]['geometry']['coordinates'])):
                for k in range(len(output_data["features"][i]['geometry']['coordinates'][j])):
                    coords = []
                    for lng, lat in TRANSFORMER.itransform(output_data["features"][i]['geometry']['coordinates'][j][k]):
                        coords.append([round(lng, 4), round(lat, 4)])
                    output_data["features"][i]['geometry']['coordinates'][j][k] = coords

    with open("converted_boundaries.json", "w") as file:
        json.dump(output_data, file)

if __name__ == '__main__':
    convert("unformatted_postal_code_boundaries.json")
