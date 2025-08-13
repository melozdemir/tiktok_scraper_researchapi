#!/usr/bin/env python3

# Aufruf mit python3 "DATEINAME"
import requests

url = "https://open.tiktokapis.com/v2/oauth/token/"

payload = 'CLIENT_KEY=awy9wlyhdqz0alvd&CLIENT_SECRET=xvCyJ9LbMk4fItpRsf2B2ir7eDwoLqrB&grant_type=client_credentials'
headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
