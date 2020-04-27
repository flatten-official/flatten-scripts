import drawSvg as draw

#all the retrieval stuff
region = "Mogadishu"
potential = 69
vulnerable = 69
high_risk = 69
total = 100
need = "need"
self_iso = 69

d = draw.Drawing(600,500,origin = (0,0))
d.append(draw.Rectangle(0,0,600,500, fill='#fbfbfb'))

var= "Mogadishu"
d.append(draw.Text("My neighbourhood in "+region,20,300,450,center=True,fill="#000000"))

d.append(draw.Rectangle(25,200,166.66667,200,fill = "#dddddd"))
d.append(draw.Rectangle(216.6667,200,166.66667,200,fill = "#dddddd"))
d.append(draw.Rectangle(408.3333,200,166.66667,200,fill = "#dddddd"))

d.append(draw.Text("Potential Cases",12,107,375,center=True,fill="#000000",font_weight="bold"))
d.append(draw.Text("Reported",12,107,360,center=True,fill="#000000",font_weight="bold"))
d.append(draw.Text("Vulnerable Individuals",12,300,375,center=True,fill="#000000",font_weight="bold"))
d.append(draw.Text("Reported",12,300,360,center=True,fill="#000000",font_weight="bold"))
d.append(draw.Text("High Risk Potential",12,493,375,center=True,fill="#000000",font_weight="bold"))
d.append(draw.Text("Cases Reported",12,493,360,center=True,fill="#000000",font_weight="bold"))

d.append(draw.Text(str(potential*100//total)+"%",50,107,300,center=True,fill="#000000"))
d.append(draw.Text(str(vulnerable*100//total)+"%",50,300,300,center=True,fill="#000000"))
d.append(draw.Text(str(high_risk*100//total)+"%",50,493,300,center=True,fill="#000000"))


d.append(draw.Text("Total Reports: "+str(total),10,107,230,center=True,fill="#000000"))
d.append(draw.Text("Total Reports: "+str(total),10,300,230,center=True,fill="#000000"))
d.append(draw.Text("Total Reports: "+str(total),10,493,230,center=True,fill="#000000"))

d.append(draw.Line(25, 175, 575, 175,stroke="#dddddd", stroke_width=2, fill='none')) 

d.append(draw.Text("Greatest Need In Your Community",12,150,145,center=True,fill="#000000",font_weight="bold"))
if need == "financialSupport":
  d.append(draw.Text("Financial",25,110,85,center=True,fill="#000000",font_weight="bold"))
  d.append(draw.Text("Support",25,110,55,center=True,fill="#000000",font_weight="bold"))
elif need == "emotionalSupport":
  d.append(draw.Text("Emotional",25,110,85,center=True,fill="#000000",font_weight="bold"))
  d.append(draw.Text("Support",25,110,55,center=True,fill="#000000",font_weight="bold"))
elif need == "medication":
  d.append(draw.Text("Medication",25,110,75,center=True,fill="#000000",font_weight="bold"))
elif need == "food":
  d.append(draw.Text("Food/",25,110,105,center=True,fill="#000000",font_weight="bold"))
  d.append(draw.Text("Necessary",25,110,75,center=True,fill="#000000",font_weight="bold"))
  d.append(draw.Text("Resources",25,110,45,center=True,fill="#000000",font_weight="bold"))
else:
  d.append(draw.Text("Need",25,110,75,center=True,fill="#000000",font_weight="bold"))
  

d.append(draw.Text("Individuals Self Isolating",12,450,145,center=True,fill="#000000",font_weight="bold"))
d.append(draw.Text(str(potential*100//total)+"%",50,425,75,center=True,fill="#000000"))



d.setPixelScale(2)  # Set number of pixels per geometry unit
#d.setRenderSize(400,200)  # Alternative to setPixelScale
d.saveSvg('fsa_card.svg')
d.savePng('fsa_card.png')
