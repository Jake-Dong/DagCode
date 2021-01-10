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

# 신발 정보 가져오는 함수
def get_shoes_info(b_name, page, **kwargs):

    if b_name == "MLB":
        b_name = "엠엘비"
    elif b_name == 'BSQT':
        b_name = "비에스큐티"
    elif b_name == "SNRD":
        b_name = "에스엔알디"
        
    shosex = ['남성용', '여성용', '남녀공용']

    danacate = [['슬립온'], ['몽크스트랩'], ['펌프스'], ['플랫'], ['샌들'], ['슬리퍼']
        , ['런닝화', '트레일런닝화', '워킹화', '마라톤화']
        , ['릿지화', '축구화', '탁구화', '운동화', '농구화', '스니커즈', '복싱화', '아쿠아트레킹화', '볼링화', '아쿠아슈즈', '트레이닝화', '테니스화', '배드민턴화',
           '인조잔디화', '포인트화', '경등산화', '중등산화', '트레킹화', '야구화']
        , ['부츠', '워커'], ['로퍼', '옥스퍼드', '컴포트화', '모카신']]
        
    musincate = ['캔버스', '구두', '힐', '플랫', '샌들', '슬리퍼', '러닝화', '스니커즈', '부츠', '로퍼']
    
    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)

    # 크롤링한 신발들의 정보를 담을 리스트
    shoes_full_info = []

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
        prod_ids = driver.find_elements_by_class_name('relation_goods_unit')
        prod_names = driver.find_elements_by_class_name('click_log_product_standard_title_')
        prod_infos = driver.find_elements_by_class_name('spec_list')
        prod_costs = driver.find_elements_by_class_name('click_log_product_standard_price_')
        prod_imgs = driver.find_elements_by_class_name('click_log_product_standard_img_')
        for q,w,e,r,t in zip(prod_ids,prod_names,prod_infos,prod_costs,prod_imgs):
            #   이미지 src를 text로 추출
            prod_img_src = t.get_attribute('src').split('?')[0]
            #   정보에서 추출
            prod_info = e.text
            prod_gender = ''
            prod_heel_size = ''
            prod_category = ''
            #   신발 성별 추출
            for n in shosex:
                if n in prod_info:
                    prod_gender = n
            if prod_gender not in shosex:
                continue
            #   굽 추출
            splitinfo = prod_info.split('/')
            for n in splitinfo:
                if ' 총굽: ' in n:
                    prod_heel_size = n.strip()[3:]
            #   카테고리 무신사기준으로 변경
            for n in range(0, len(danacate)):
                for m in range(0, len(danacate[n])):
                    if danacate[n][m] in prod_info:
                        prod_category = musincate[n]
            if prod_category == '':
                continue
            
            #id, 모델명, 품번
            prod_id = q.get_attribute('id')[20:]
            prod_name = w.text
            prod_shono = ''
            tmp_name = prod_name.split(' ')
            for n in tmp_name:
                if re.match('.*[a-zA-Z]*.*\d+.*', n):
                    prod_shono = n
                    prod_name = ' '.join(tmp_name[1:tmp_name.index(n)])
                    
            #   가격추출 
            prod_cost = r.text
            prod_cost = ''.join(prod_cost[:-1].split(','))
            
            if (prod_name == ' ') | (prod_name == ''):
                prod_name = prod_shono

            shoes_full_info.append([b_name, prod_id, prod_shono, prod_name, prod_category, prod_gender, prod_heel_size, prod_cost, prod_img_src])
                
    driver.close()
    
    danawa = pd.DataFrame(
          data=shoes_full_info
        , columns=['brand','danawa_id','shono','modelname','category','shosex','heelsize','price_d', 'img_src']
    )
    danawa.to_csv(f'/root/reviews/danawa_{b_name}_id.csv')
    # 마리아디비로 전송
    engine = create_engine("mysql+mysqldb://footfootbig:" + "footbigmaria!" + "@35.185.210.97/footfoot" + "?charset=utf8mb4")
    conn = engine.connect()
    try:
        danawa.to_sql(name='danawa_shoes', con=engine, if_exists='append', index=False)
    finally:
        conn.close()

def get_b_name_page(count, **kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')
    try:
        with conn.cursor() as curs:

            select_brand = """
                SELECT brand, page
                  FROM danawa_brand
                 WHERE idx=%s;
            """
            curs.execute(select_brand, count)
            b_name, page = curs.fetchone()
    finally:
        conn.close()
        
    get_shoes_info(b_name, page)

def truncate(**kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')
    try:
        with conn.cursor() as curs:
            truncate_table = """
                truncate table danawa_shoes;
            """
            curs.execute(truncate_table)
    finally:
        conn.close()
        
def xcom_push(**kwargs):
    kwargs['ti'].xcom_push(key='danawa_id_crawling_end', value=True)

#--------------------------------에어 플로우 코드----------------------------------#

def check_id_start_notify(**kwargs):
    check = False
    while not check:
        try:
            check = kwargs['ti'].xcom_pull(key='id_crawling_start', dag_id='line_notify_id_crawling')
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
    'start_date': datetime(today.year, today.month, today.day, tzinfo=local_tz) - timedelta(days=15),
    'provide_context': True,
    'retries': 2,
    'retry_delay':timedelta(minutes=1),
    'catchup': False
}    
    
# DAG인스턴스 생성
dag = DAG(
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='danawa_id_crawling_to_sql'
    # DAG 설정을 넣어줌
    , default_args=default_args
    # 최대 실행 횟수
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(days=14)
)

# 시작 감지
check_id_start_notify = PythonOperator(
    task_id='check_id_start_notify',
    python_callable=check_id_start_notify,
    dag=dag,
)

# 테이블 초기화 DAG
truncate = PythonOperator(
    task_id='truncate',
    python_callable=truncate,
    dag=dag,
)

# 테이블 초기화 DAG
xcom_push = PythonOperator(
    task_id = 'xcom_push',
    python_callable = xcom_push,
    dag = dag,
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
    check_id_start_notify >> truncate >> id_crawling >> xcom_push

