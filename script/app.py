import requests

import test
url = "https://dadosabertos.tse.jus.br/dataset/resultados-2022-arquivos-transmitidos-para-totalizacao"
url = "https://dadosabertos.tse.jus.br/dataset/resultados-2024-arquivos-transmitidos-para-totalizacao"

response = requests.get(url)

soup = BeautifulSoup(response.content, "html.parser")

ul = soup.find('ul', class_='resource-list')
lis = ul.find_all("li")