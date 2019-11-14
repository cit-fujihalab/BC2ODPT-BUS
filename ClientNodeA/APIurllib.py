# coding: utf-8
import urllib.request
import json
import zipfile
consumer_key = "4e35e7656b430c594a80294f6fe4061a914a54640a8f270691b4f04a9285d215"
bus_route = "odpt.Busroute:Toei.Ou40Kou"
# url: https://api-tokyochallenge.odpt.org/api/v4/odpt:Bus?acl:consumerKey=25ceb1028f6fb35f2f075ada0f878bd92e7a674fb453752922d72026cd0cc81d&odpt:busroute=odpt.Busroute:Toei.Ou40Kou

# access Web API to get open data
#        url = "https://api-tokyochallenge.odpt.org/api/v4/odpt:Bus?acl:consumerKey=" + consumer_key + "&odpt:busroute=" + bus_route

#url = "https://api-tokyochallenge.odpt.org/api/v4/odpt:Bus?acl:consumerKey=" + consumer_key + "&odpt:busroute=" + bus_route
# url = "https://api-tokyochallenge.odpt.org/api/v4/odpt:Bus?acl:consumerKey=" + consumer_key + bus_route
url = "https://api-tokyochallenge.odpt.org/api/v4/odpt:Bus?acl:consumerKey=" + consumer_key + "&odpt:busroute=" + bus_route
req = urllib.request.Request(url)

with urllib.request.urlopen(req) as res:
    body = res.read().decode()

print(body)


