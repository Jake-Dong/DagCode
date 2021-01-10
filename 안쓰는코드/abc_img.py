import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import re
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
import time
import csv
import pandas as pd
import numpy as np
#  abc 운동화

options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('window-size=1920x1080')
options.add_argument("disable-gpu")
options.add_argument(f'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
driver = webdriver.Chrome(r'C:\Users\Admin\PycharmProjects\pythonProject\chromedriver.exe',options=options)

pandas_read = pd.read_csv('ABCname.csv')['prod_id']

img_list = []
for prod_id in pandas_read.head(5):
    url = 'https://abcmart.a-rt.com/product/new?prdtNo='+str(prod_id)
    driver.get(url)
    img_html = driver.find_elements_by_tag_name('button > a > img')
    for img_attr in img_html:
        img_url = img_attr.get_attribute('src')
        img_list.append(img_url)
    n = 1
    for url in img_list:
        n = n + 1
        r = requests.get(url)
        file = open("ABC_img{}.jpg".format(str(prod_id) +str('No') +str(n) ), "wb")
        file.write(r.content)
file.close()
