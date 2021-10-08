import requests
from bs4 import BeautifulSoup
import parse_data

DIR_PATH = '/Volumes/JohnMyPassport/data_repository/VizieR_data/docs/html'
VISITED_URLS = []

def read_path(start_url):
    res = requests.get(url=start_url)
    res_url = res.url.replace(".html", "")

    VISITED_URLS.append(res.url)
    html = res.content
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find_all('a')

    save_path = "/".join([DIR_PATH, res_url.replace("https://","").replace('/', '_')])
    if not save_path.endswith('.html'):
        save_path = save_path + ".html"
        save_path = save_path.replace('_.html', '.html')

    with open(save_path, 'w') as save_file:
        save_file.write(res.text)

    for link in links:
        href = link.get('href')
        if href is None or href == '.' or '..' in href or '#' in href or 'http://' in href or href.startswith('/'):
            continue
        url = "".join([res.url, href])
        if url in VISITED_URLS:
            continue
        print(f"HREF = {href} | url {url}")
        if not res.url.endswith(".html"):
            read_path(url)


if __name__ == '__main__':
    # read_path('https://gea.esac.esa.int/archive/documentation/GEDR3/Gaia_archive/chap_datamodel/')
    parse_data.parse(DIR_PATH)
