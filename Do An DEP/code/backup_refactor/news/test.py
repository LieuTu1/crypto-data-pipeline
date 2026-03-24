import requests
from bs4 import BeautifulSoup

# 1️⃣ URL Binance Bitcoin News
url = "https://www.binance.com/vi/square/news/bitcoin-news"

# 2️⃣ Headers (giữ nguyên, giống trình duyệt)
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "vi,vi-VN;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
    "cache-control": "max-age=0",
    "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1"
}

# 3️⃣ Cookie từ trình duyệt (dán nguyên chuỗi của bạn vào đây)
cookies = "monitor-uid=1193689913; bnc-uuid=b5d0bb43-b813-4890-90b3-4161e144d42b; lang=vi; se_gd=AVQCgVhkEQQCA0KIHBFRgZZUxBxUFBRVFMGFRVEJFRWVADlNWVcS1; se_gsd=eyohLEJ8NTMiCQkmIQM1IzU0BxMEAgAaV1hHVFJbVVRQI1NT1; BNC_FV_KEY=337fda9588baa609abf6b2c144ddec97003ad50b; aws-waf-token=04c6d0bc-170f-453e-b9cb-261d56a3d8b9:BgoAek8hjMNCAAAA:D1E+x036v/mUhQZItEhGj6pXOaie6J8fwxmUGcBahGQ184RfVlZxgefE5MlsTSY9Jj1ATxA3kr5bfb8CxZw5YdymcaVsaSSVy5hUM2HJ9wVsItEXas20VaVP2aRWs0Pj0em5MwgYP22+W8XZNKzADP9wFaNCxdCQuSZJfI9Yb6068abJ3w3Jf51hjY7EH8nW9T4=; ... (dán tiếp toàn bộ chuỗi cookie)"

# 4️⃣ Chuyển cookie string sang dict an toàn
cookies_dict = {}
for c in cookies.split('; '):
    if '=' in c:
        key, value = c.split('=', 1)  # chỉ tách lần đầu
        cookies_dict[key] = value

# 5️⃣ Gửi GET request
response = requests.get(url, headers=headers, cookies=cookies_dict)
html = response.text

# 6️⃣ Parse HTML với BeautifulSoup
soup = BeautifulSoup(html, 'html.parser')

# 7️⃣ Lấy danh sách tin: thời gian + tiêu đề
cards = soup.find_all("div", class_="css-vurnku")  # thẻ chứa từng tin

for card in cards:
    # time_raw: tìm div.data-bn-type="text" + class css-vyak18
    time_node = card.select_one('div[data-bn-type="text"].css-vyak18')
    time_raw = time_node.text.strip() if time_node else "--"

    # title: thẻ h3 hoặc div chứa tiêu đề
    title_node = card.select_one('h3') or card.select_one('div.css-yxpvu')
    title = title_node.text.strip() if title_node else "--"

    print(time_raw, title)
