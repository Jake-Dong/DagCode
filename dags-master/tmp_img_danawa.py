# crawling
from bs4 import BeautifulSoup
from selenium import webdriver
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

def get_danawa_brand_count(**kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')
    try:
        with conn.cursor() as curs:
            select_count = """
                SELECT count(*) from danawa_brand;
            """
            curs.execute(select_count)
            count = curs.fetchone()[0]
            
    finally:
        conn.close()

    return count + 1

#--------------------------------크롤링 코드----------------------------------#

def get_b_name_page(count, **kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')
    try:
        with conn.cursor() as curs:

            select_brand = """
                SELECT page
                  FROM danawa_brand
                 WHERE idx=%s;
            """
            curs.execute(select_brand, count)
            page = curs.fetchone()[0]
            
            # 크롬 드라이버 옵션
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-gpu')
            options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
            driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)

            # 리뷰 많은 순으로 정렬하여 15페이지까지만 진행
            for i in range(1,16):
                url = 'http://search.danawa.com/dsearch.php?query=%EC%8B%A0%EB%B0%9C&originalQuery=%EC%8B%A0%EB%B0%9C&previousKeyword=%EC%8B%A0%EB%B0%9C&volumeType=allvs&page='+str(i)+'&limit=120&sort=opinionDESC&list=list&boost=true&addDelivery=N&brand='+str(page)+'&tab=main'
                driver.get(url)
                time.sleep(3)
                driver.implicitly_wait(60)
                try:
                    nosearchArea = driver.find_element_by_selector('#nosearchArea')
                    break
                except:
                    pass
                # 모델 코드, 모델 이름, 모델 정보
                prod_names = driver.find_elements_by_class_name('click_log_product_standard_title_')
                prod_imgs = driver.find_elements_by_class_name('click_log_product_standard_img_')
                for w in prod_names:
                    prod_name = w.text
                    prod_shono = ''
                    tmp_name = prod_name.split(' ')
                    for n in tmp_name:
                        if re.match('.*[a-zA-Z]*.*\d+.*', n):
                            prod_shono = n
                            prod_name = ' '.join(tmp_name[1:tmp_name.index(n)])
                    if (prod_name == ' ') | (prod_name == ''):
                        prod_name = prod_shono     
            driver.close()
    finally:
        conn.close()

#--------------------------------에어 플로우 코드----------------------------------#

# 서울 시간 기준으로 변경
local_tz = pendulum.timezone('Asia/Seoul')
today = datetime.today()
# airflow DAG설정        
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(today.year, today.month, today.day, tzinfo=local_tz) - timedelta(days=15),
    'provide_context': True,
    'catchup': False
}    
    
# DAG인스턴스 생성
dag = DAG(
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='tmp_danawa_img_crawling_to_sql'
    # DAG 설정을 넣어줌
    , default_args=default_args
    # 최대 실행 횟수
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(days=14)
)

# DAG 동적 생성
# 크롤링 DAG
count = get_danawa_brand_count()

for count in range(1, count):
    id_crawling = PythonOperator(
        task_id='{0}_id_crawling'.format(count),
        python_callable=get_b_name_page,
        op_kwargs={'count':count},
        dag=dag
    )
    id_crawling

