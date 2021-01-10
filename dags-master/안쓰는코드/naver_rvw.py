'''
import pandas as pd
import numpy
from selenium import webdriver
import re
import time
import csv
import datetime as dt
from tqdm import tqdm
import requests

# 네이버



dt = dt.datetime.now()
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-gpu')
options.add_argument(
    '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)

naver_brand ={'나이키': 1269, '아디다스': 667, '탠디': 14407, '뉴발란스': 10946, 'XENIA':13840, '소다':759, '고세':10694, '엘칸토':544, '에스콰이아':13181
    , '미소페':11877, '푸마':126, '스케쳐스':12606, '닥스':1217, '크록스':29750, '반스':923, '리복':1056, '락포트':11294, '무크':135173, '발렌티노':12004
    , '아식스':654, '휠라':7, '컨버스':14153, '세라':12439, '프로스펙스':14723, '핏플랍':157267, '구찌':10716, '골든구스':178717, '제옥스':30347, '프라다':14696
    , '어그':13109, '베어파우':12270, '토즈':14445, '발렌시아가':920, '미즈노':946, '라코스테':1149, '언더아머':32926, '알렉산더맥퀸':202509, '금강제화':228910
    , '타미힐피거':240, 'BABARA':11944, '르까프':11489, '살바토레페라가모':14623, '포멜카멜레':237949, '닥터마틴':1216, '페이유에':202618, '토리버치':32809, '수페르가':154988
    , '잭앤질':20408, '슈펜':208870, '스코노':725, '마이클코어스':1392, '게스': 1326, '프레드페리':30259, '메종마르지엘라':214884, '스퍼':110112, '월드컵':475, '버버리':12034
    , '오니츠카타이커':31786, '코치':14223, 'ASH':176654, '미우미우':11893, '탐스':14455, '케즈':30900, '쎄리스':12442, '카파':327, '아떼바네사브루노':198715
    , '아키클래식':159296, '커먼프로젝트':200285, '캠퍼':199402, '브룩스':20895, '엑셀시오르':228443, '스티유':176471, '레페토':202451, '데상트':136246, '배롭':209297
    , '질바이질스튜어트':149383, '르꼬끄스포르티브':17271, '폴로':133, '디올':14298, '디스커버리익스페디션':29907, '호카네오네':197164, '에르메스':13143, '스닉솔':225402
    , '스프리스':12671, '샤네르꼼데가르송':12401, '슬레진저':202616, '슈마커':202670, '슈콤마보니':248853, 'MLB':32574, '오즈웨어':539, '부테로':185786, '블랙마틴싯봉':202521
    , '찰스앤키스':178089, '폴로키즈':206141, '벤시몽':132, '럽썸':29269, '다이나핏':222473, '나무하나':244494, '내셔널지오그래픽':138687}

url_list = []
for brand_name_list,brand_num_list in naver_brand.items():
    naver_info_and_rvw = []
    for page in range(15):
        url = 'https://search.shopping.naver.com/search/all?brand='+str(brand_num_list)+'&origQuery=%EC%8B%A0%EB%B0%9C&pagingIndex=' + str(page) + '&pagingSize=80&productSet=model&query=%EC%8B%A0%EB%B0%9C&sort=review&timestamp=&viewType=list'
        driver.get(url)
        time.sleep(3)
        prod_url_list = driver.find_elements_by_xpath(
            '//*[@id="__next"]/div/div[2]/div[2]/div[3]/div[1]/ul/div/div/li/div/div[2]/div[1]/a')
        for prod_url_attr in prod_url_list:
            base_url = prod_url_attr.get_attribute('href')
            url_list.append(base_url)
            for prod_url in url_list:
                driver.get(prod_url)  # get = 이동시키는 역할
                time.sleep(3)
                driver.implicitly_wait(10)
                prod_name = driver.find_element_by_css_selector(
                    '#container > div.summary_area > div.summary_info._itemSection > div > div.h_area > h2')
                prod_name_text = prod_name.text
                brand = driver.find_element_by_css_selector(
                    '#container > div.summary_area > div.summary_info._itemSection > div > div.goods_info > div > span:nth-child(2) > em')
                brand_text = brand.text


                # 네이버 대표 이미지 가져와서 현재 디렉토리에 저장하는 코드(디렉토리 설정해주세요.)
                prod_main_img = driver.find_element_by_css_selector('#viewImage')
                img_url = prod_main_img.get_attribute('src')
                r = requests.get(img_url)
                file = open("naver_img_{}.jpg".format(str(prod_name_text)), "wb")
                file.write(r.content)
                file.close()


                all_review_counts = driver.find_element_by_css_selector('#snb > ul > li.mall_review > a > em')
                end_page = int(all_review_counts.text) / 20
                try:
                    for page in range(1, int(end_page)):
                        driver.execute_script(f"shop.detail.ReviewHandler.page({page+1}, '_review_paging'); return false;")
                        time.sleep(5)
                except:
                    pass
                    prod_infos = driver.find_elements_by_css_selector(
                        '#_review_list > li > div > div.avg_area > span > span:nth-child(4)')
                    review_dates = driver.find_elements_by_css_selector(
                        '#_review_list > li > div > div.avg_area > span > span:nth-child(3)')
                    reviews = driver.find_elements_by_css_selector('#_review_list > li > div > div.atc')
                    for review_date, prod_info, review in zip(review_dates, prod_infos, reviews):
                        review_date_text = review_date.text
                        prod_info_text = prod_info.text
                        review_text = review.text
                        naver_info_and_rvw.append([brand_text, prod_name_text, review_date_text, prod_info_text, review_text])

            refilename = f'/root/reviews/naver_{brand_name_list}.csv'
            f = open(refilename, 'w', encoding='utf-8', newline='')
            csvWriter = csv.writer(f)
            csvWriter.writerow(['brand', 'prod_name', 'review_info','review_date', 'reviews'])
            for w in naver_info_and_rvw:
                csvWriter.writerow(w)
            f.close()
'''