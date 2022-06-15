def get_random_proxy():
    import re, requests
    from bs4 import BeautifulSoup
    from random import choice
    IP_PATTERN = r'\d+\.'*3 + '\d+'
    PORT_PATTERN = r'^\d+$'
    ips, ports = [], []
    r = requests.get('https://free-proxy-list.net/')
    soup = BeautifulSoup(r.text,'html.parser')
    for td in soup.find('table').find_all('td'):
        if re.search(IP_PATTERN, td.text):
            ips.append(td.text)
        elif re.search(PORT_PATTERN, td.text):
            ports.append(td.text)
    addresses = [f'{ip}:{port}' for ip,port in zip(ips,ports)]
    return choice(addresses)