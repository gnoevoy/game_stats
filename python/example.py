from bs4 import BeautifulSoup
import requests


url = "https://stats.bc18go.ru/hlstats.php?mode=players&game=css&sort=skill&sortorder=desc&page=1"
response = requests.get(url)
r = response.content
soup = BeautifulSoup(r, "html.parser")

content = soup.find_all("table", class_="data-table")
print(content)
raise KeyError

idx = 1
for row in content[0].find_all("tr"):
    print(idx, row.text.strip())
    idx += 1
