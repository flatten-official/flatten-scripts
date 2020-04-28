import drawSvg as draw
import json

def get_fsa_to_name():
  with open('fsa_name.json') as f:
    data = json.load(f)
  return data

#rx=20 ry=20
def make_image_card(FSA,potential,vulnerable,high_risk,total,need,self_iso):
  translations = get_fsa_to_name()
  region = translations[FSA]

  d = draw.Drawing(600,600,origin = (0,0))
  d.append(draw.Rectangle(0,0,600,600, fill='#FFFFFF'))

  d.append(draw.Text("My neighbourhood in "+region,20,300,550,center=True,fill="#000000"))

  d.append(draw.Rectangle(25,300,166.66667,200,rx = 10,ry=10, fill = "#F5F3F2"))
  d.append(draw.Rectangle(216.6667,300,166.66667,200,rx = 10,ry=10,fill = "#F5F3F2"))
  d.append(draw.Rectangle(408.3333,300,166.66667,200,rx = 10,ry=10,fill = "#F5F3F2"))

  d.append(draw.Text("POTENTIAL CASES",10,107,475,center=True,fill="#000000",font_weight="bold"))
  d.append(draw.Text("REPORTED",10,107,460,center=True,fill="#000000",font_weight="bold"))
  d.append(draw.Text("VULNERABLE INDIVIDUALS",10,300,475,center=True,fill="#000000",font_weight="bold"))
  d.append(draw.Text("REPORTED",10,300,460,center=True,fill="#000000",font_weight="bold"))
  d.append(draw.Text("HIGH RISK POTENTIAL",10,493,475,center=True,fill="#000000",font_weight="bold"))
  d.append(draw.Text("CASES REPORTED",10,493,460,center=True,fill="#000000",font_weight="bold"))

  d.append(draw.Text(str(potential*100//total)+"%",50,107,400,center=True,fill="#000000",font_weight="bold"))
  d.append(draw.Text(str(vulnerable*100//total)+"%",50,300,400,center=True,fill="#000000",font_weight="bold"))
  d.append(draw.Text(str(high_risk*100//total)+"%",50,493,400,center=True,fill="#000000",font_weight="bold"))


  d.append(draw.Text("Total Reports: "+str(total),10,107,330,center=True,fill="#000000"))
  d.append(draw.Text("Total Reports: "+str(total),10,300,330,center=True,fill="#000000"))
  d.append(draw.Text("Total Reports: "+str(total),10,493,330,center=True,fill="#000000"))

  d.append(draw.Line(25, 275, 575, 275,stroke="#F5F3F2", stroke_width=2, fill='none')) 

  d.append(draw.Text("GREATEST NEED IN YOUR COMMUNITY",10,150,245,center=True,fill="#000000",font_weight="bold"))
  if need == "financialSupport":
    d.append(draw.Text("Financial",20,100,200,center=True,fill="#000000",font_weight="bold"))
    d.append(draw.Text("Support",20,100,170,center=True,fill="#000000",font_weight="bold"))
    d.append(draw.Image(175,150,70,70,path="Money Icon.png"))
  elif need == "emotionalSupport":
    d.append(draw.Text("Emotional",20,100,200,center=True,fill="#000000",font_weight="bold"))
    d.append(draw.Text("Support",20,100,170,center=True,fill="#000000",font_weight="bold"))
    d.append(draw.Image(180,150,70,70,path="Heart Icon.png"))
  elif need == "medication":
    d.append(draw.Text("Medication",20,100,185,center=True,fill="#000000",font_weight="bold"))
    d.append(draw.Image(190,150,70,70,path="Health Icon.png"))
  elif need == "food":
    d.append(draw.Text("Food/",20,100,215,center=True,fill="#000000",font_weight="bold"))
    d.append(draw.Text("Necessary",20,100,185,center=True,fill="#000000",font_weight="bold"))
    d.append(draw.Text("Resources",20,100,155,center=True,fill="#000000",font_weight="bold"))
    d.append(draw.Image(190,150,70,70,path="Food Icon.png"))
  else:
    d.append(draw.Text(need,20,100,185,center=True,fill="#000000",font_weight="bold"))

  d.append(draw.Text("INDIVIDUALS IN SELF ISOLATION",10,450,245,center=True,fill="#000000",font_weight="bold"))
  d.append(draw.Text(str(self_iso*100//total)+"%",40,410,190,center=True,fill="#000000",font_weight="bold"))
  d.append(draw.Image(470,150,70,70,path="House Icon.png"))

  d.append(draw.Line(25, 125, 575, 125,stroke="#F5F3F2", stroke_width=2, fill='none')) 
  d.append(draw.Image(100,0,400,70,path="flatten.png"))
  d.append(draw.Text("Produced by",20,300,100,center=True,fill="#000000"))

  d.setPixelScale(2)  # Set number of pixels per geometry unit
  #d.setRenderSize(400,200)  # Alternative to setPixelScale
  #d.saveSvg('fsa_card.svg')
  d.savePng('fsa_card.png')

if __name__ == "__main__":
  FSA = "K1R"
  potential = 69
  vulnerable = 69
  high_risk = 69
  total = 100
  need = "emotionalSupport"
  self_iso = 69
  make_image_card(FSA,potential,vulnerable,high_risk,total,need,self_iso)