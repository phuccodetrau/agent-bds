import requests

reponses = requests.get("https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text")

reponses_text = reponses.text
lines = reponses_text.split("\n")
list_ips = []
for line in lines:
    parts = line.split("/")
    if parts[0] == "http:":
        list_ips.append(parts[-1])
with open("proxies.txt", "w", encoding="utf-8") as f:
    for ip_addr in list_ips:
        f.write(ip_addr)