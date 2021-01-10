# crawling
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import re
import time
import csv
import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine

# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import pendulum
import requests

#--------------------------------실행 초기 설정 코드----------------------------------#

def get_naver_brand_count(**kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:
            select_count = """
                SELECT count(*) from naver_brand;
            """
            curs.execute(select_count)
            counts = curs.fetchone()[0]

    finally:
        conn.close()

    return counts + 1
    
#--------------------------------크롤링 코드----------------------------------#

# 신발 정보 가져오는 함수
def get_shoes_review(b_name, page, **kwargs):

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    naver_info_and_rvw = []
    for page in range(15):
        url = 'https://search.shopping.naver.com/search/all?brand='+str(b_name)+'&origQuery=%EC%8B%A0%EB%B0%9C&pagingIndex=' + str(page) + '&pagingSize=80&productSet=model&query=%EC%8B%A0%EB%B0%9C&sort=review&timestamp=&viewType=list'
        driver.get(url)
        time.sleep(3)
        url_list = []
        prod_url_list = driver.find_elements_by_xpath(
            '//*[@id="__next"]/div/div[2]/div[2]/div[3]/div[1]/ul/div/div/li/div/div[2]/div[1]/a')
        for prod_url_attr in prod_url_list:
            base_url = prod_url_attr.get_attribute('href')
            url_list.append(base_url)
        for url in url_list:
            driver.get(url)  # get = 이동시키는 역할
            time.sleep(2)
            driver.implicitly_wait(10)
            prod_name = driver.find_element_by_css_selector(
                '#container > div.summary_area > div.summary_info._itemSection > div > div.h_area > h2')
            prod_name_text = prod_name.text
            brand = driver.find_element_by_css_selector(
                '#container > div.summary_area > div.summary_info._itemSection > div > div.goods_info > div > span:nth-child(2) > em')
            brand_text = brand.text

            # 네이버 대표 이미지 가져와서 현재 디렉토리에 저장하는 코드(디렉토리 설정해주세요.)
            #prod_main_img = driver.find_element_by_css_selector('#viewImage')
            #img_url = prod_main_img.get_attribute('src')
            #r = requests.get(img_url)
            #file = open("naver_img_{}.jpg".format(str(prod_name_text)), "wb")
            #file.write(r.content)
            #file.close()

            all_review_counts = driver.find_element_by_css_selector('#snb > ul > li.mall_review > a > em')
            end_page = int(all_review_counts.text) / 20
            try:
                for page in range(1, int(end_page)):
                    driver.execute_script(f"shop.detail.ReviewHandler.page({page+1}, '_review_paging'); return false;")
                    time.sleep(5)
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
            except:
                pass
    refilename = f'/root/reviews/naver_{b_name}.csv'
    f = open(refilename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['brand', 'prod_name', 'review_info','review_date', 'reviews'])
    for w in naver_info_and_rvw:
        csvWriter.writerow(w)
    f.close()

def get_b_name_page(count, **kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:

            select_brand = """
                SELECT brand, num
                  FROM naver_brand
                 WHERE idx=%s;
            """
            curs.execute(select_brand, count)
            b_name, page = curs.fetchone()

            get_shoes_review(b_name, page)

    finally:
        conn.close()


def truncate(**kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')
    try:
        with conn.cursor() as curs:
            truncate_table = """
                truncate table naver_shoes;
            """
            curs.execute(truncate_table)
    finally:
        conn.close()
        
def xcom_push(**kwargs):
    kwargs['ti'].xcom_push(key='naver_shopping_crawling_end', value=True)
        
#--------------------------------에어 플로우 코드----------------------------------#

def check_review_start_notify(**kwargs):
    check = False
    while not check:
        try:
            check = kwargs['ti'].xcom_pull(key='review_crawling_start', dag_id='line_notify_review_crawling')
        except:
            pass
        if not check:
            time.sleep(60*5)

# 서울 시간 기준으로 변경
local_tz = pendulum.timezone('Asia/Seoul')
today = datetime.today()
# airflow DAG설정        
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(today.year, today.month, today.day, tzinfo=local_tz) - timedelta(hours=25),
    'catchup': False,
    'provide_context': True
}    
    
# DAG인스턴스 생성
dag = DAG(
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='naver_id_review_crawling'
    # DAG 설정을 넣어줌
    , default_args=default_args
    # 최대 실행 횟수
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(days=1)
)

# 테이블 초기화 DAG
truncate = PythonOperator(
    task_id='truncate',
    python_callable=truncate,
    dag=dag,
)

check_review_start_notify = PythonOperator(
    task_id='check_review_start_notify',
    python_callable=check_review_start_notify,
    dag=dag
)
# 테이블 초기화 DAG
xcom_push = PythonOperator(
    task_id = 'xcom_push',
    python_callable = xcom_push,
    dag = dag,
)

# DAG 동적 생성
# 크롤링 DAG
counts = get_naver_brand_count()

for count in range(1, counts):
    id_crawling = PythonOperator(
        task_id='{0}_id_crawling'.format(count),
        python_callable=get_b_name_page,
        op_kwargs={'count':count},
        dag=dag
    )
    check_review_start_notify >> truncate >> id_crawling >> xcom_push
