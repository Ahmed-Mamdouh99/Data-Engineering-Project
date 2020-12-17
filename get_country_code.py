import requests

url = 'https://restcountries.eu/rest/v2/name/{}'
names = ["Saint Lucia",
"Slovakia",
"Iran (Islamic Republic of)",
"Gambia",
"Micronesia (Federated States of)",
"United States of America",
"Bahamas",
"Lao People's Democratic Republic",
"Republic of Moldova",
"Cook Islands",
"Saint Vincent and the Grenadines",
"Yemen",
"United Kingdom of Great Britain and Northern Ireland",
"The former Yugoslav republic of Macedonia",
"Côte d'Ivoire",
"Niue",
"United Republic of Tanzania",
"Saint Kitts and Nevis",
"Bolivia (Plurinational State of)",
"Congo",
"Czechia",
"Venezuela (Bolivarian Republic of)",
"Republic of Korea",
"Democratic People's Republic of Korea",
"Democratic Republic of the Congo",
"Egypt",
"Viet Nam",
"Kyrgyzstan"]
not_found = []
found = []
for name in names:
  r = requests.get(url=url.format(name))
  data = r.json()
  if isinstance(data, dict):
    not_found.append([name, data.get('status')])
  else:
    hit = data[0]
    found.append([name, hit.get('name'), hit.get('alpha3Code')])
print(not_found)
print(found)