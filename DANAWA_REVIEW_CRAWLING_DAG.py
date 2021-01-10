# common
import re
import time
import requests
from datetime import datetime, timedelta

# crawling
from bs4 import BeautifulSoup
from selenium import webdriver
import csv

# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import pendulum
import pymysql

# preprocessing
import pandas as pd
import numpy as np
from ks4r.ks4r import Summarizer

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
            counts = curs.fetchone()[0]

    finally:
        conn.close()

    return counts + 1

def date_check(**kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:
            sql = """
                SELECT * from lastcrawling;
            """
            curs.execute(sql)
            last_date = curs.fetchone()[0]

    finally:
        conn.close()
    # 지난 실행일 전날의 23시59분59초 부터 이번 실행일 전날의 23시59분59초 까지의 리뷰를 수집
    limit_date = datetime.today() - timedelta(days=1)
    limit_date = limit_date.replace(hour=23,minute=59,second=59)
    last_date = last_date - timedelta(days=1)
    last_date = last_date.replace(hour=23,minute=59,second=59)
    return last_date, limit_date

#--------------------------------크롤링 코드----------------------------------#

def get_shoes_review(b_name, prod_ids, last_excute_date, limit_date, **kwargs):

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)

    for prod_id in prod_ids:
        conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!', database='footfoot')
        try:
            with conn.cursor() as curs:
                select_count = """
                    SELECT review_count from danawa_shoes where danawa_id=%s;
                """
                curs.execute(select_count, prod_id)
                initial_count = curs.fetchone()[0]
        finally:
            conn.close()
        danawa_reviews = []
        page = 0
        review_count = 0
        while True:
            page = page + 1
            url = 'http://prod.danawa.com/info/dpg/ajax/companyProductReview.ajax.php?t=0.10499996477784657&prodCode='+str(prod_id)+'&cate1Code=1824&page='+str(page)+'&limit=100&score=0&sortType=&usefullScore=Y&innerKeyword=&subjectWord=0&subjectWordString=&subjectSimilarWordString=&_=1600608005961'
            driver.get(url)
            time.sleep(1)
            driver.implicitly_wait(10)
            rvw_date = driver.find_elements_by_xpath('/html/body/div/div[3]/div[2]/ul/li/div[1]/span[2]')
            rvw_list = driver.find_elements_by_xpath('/html/body/div/div[3]/div[2]/ul/li/div[2]/div[1]/div[2]')

            try:
                no_data = driver.find_element_by_class_name('no_data')
                if no_data != None:
                    break
            except:
                pass
            for q,w in zip(rvw_date,rvw_list):
                check_date = datetime.strptime(review_date,'%Y.%m.%d')
                if (last_excute_date < check_date) & (check_date < limit_date):
                    review_count = review_count + 1
                    filename ='/home/reviews/danawa.txt'
                    f = open(filename, 'a', encoding='utf-8', newline='')
                    review_date = q.text
                    review = w.text
                    f.write(f'{prod_id} {review_date} {review}\n')
                    f.close()
                    danawa_reviews.append([prod_id,review_date,review])
                    
        conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!', database='footfoot')
        try:
            with conn.cursor() as curs:
                if initial_count < 20:
                    total_review = []
                    for info in danawa_reviews:
                        total_review.append(info[2])
                    review4summary = '.'.join(total_review)
                    if (initial_count + review_count) > 19:
                        summarizer = Summarizer()
                        summary = '\n'.join(summarizer(review4summary))
                        set_summaries = """
                            UPDATE danawa_shoes SET summaries=%s, review_count=%s WHERE danawa_id=%s;
                        """
                        curs.execute(set_summaries, (summary, (initial_count + review_count), prod_id))
                    else:
                        get_tmp_review = """
                            SELECT tmp_review FROM danawa_shoes WHERE danawa_id=%s
                        """
                        curs.execute(get_tmp_review, (prod_id))
                        tmp_review = curs.fetchone()[0]
                        update_tmp= """
                            UPDATE danawa_shoes SET tmp_review=%s, review_count=%s WHERE danawa_id=%s;
                        """
                        curs.execute(update_tmp, ((review4summary + tmp_review), (initial_count + review_count), prod_id))
                else:
                    update_count= """
                        UPDATE danawa_shoes SET review_count=%s WHERE danawa_id=%s;
                    """
                    curs.execute(update_count, ((initial_count + review_count), prod_id))
        finally:
            conn.close()        
            
        # 확인 및 백업을 위해 로컬에 csv파일로 저장
        filename = f'/root/reviews/danawa_{prod_id}.csv'
        with open(filename, 'w', encoding='utf-8', newline='') as f:
            csvwriter = csv.writer(f)
            csvwriter.writerow(['danawa_id','review_date','review'])
            for i in danawa_reviews:
                csvwriter.writerow(i)
        
    driver.close()

def get_b_name_prod_ids(last_excute_date, limit_date, count, **kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')
    try:
        with conn.cursor() as curs:

            select_brand = """
                SELECT brand
                  FROM danawa_brand
                 WHERE idx=%s;
            """
            curs.execute(select_brand, count)
            b_name = curs.fetchone()[0]

            select_danawa_id = """
                SELECT danawa_id
                  FROM danawa_shoes
                 WHERE brand=%s;
            """
            curs.execute(select_danawa_id, b_name)
            ids = curs.fetchall()

            prod_ids = []
            for i in range(0, len(ids)):
                prod_ids.append(ids[i][0])

    finally:
        conn.close()

    get_shoes_review(b_name, prod_ids, last_excute_date, limit_date)
    
#--------------------------------크롤링 종료시 실행 코드----------------------------------#

def update_excute_date(**kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')
    try:
        with conn.cursor() as curs:
            update_date = """
                update lastcrawling set latest_date=now();
            """
            curs.execute(update_date)
            conn.commit()

    finally:
        conn.close()
        kwargs['ti'].xcom_push(key='danawa_review_crawling_end', value=True)

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
      dag_id='danawa_review_crawling_from_sql'
    # DAG 설정을 넣어줌
    , default_args=default_args
    # 최대 실행 횟수
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(days=1)
)

check_review_start_notify = PythonOperator(
    task_id='check_review_start_notify',
    python_callable=check_review_start_notify,
    dag=dag
)
update_excute_date = PythonOperator(
      task_id='update_excute_date'
    , python_callable=update_excute_date
    , dag=dag
)

# DAG 동적 생성
counts = get_danawa_brand_count()
last_excute_date, limit_date = date_check()

for count in range(1, counts):
    review_crawling = PythonOperator(
        task_id='{0}_review_crawling'.format(count),
        python_callable=get_b_name_prod_ids,
        op_kwargs={'last_excute_date':last_excute_date
                  ,'limit_date':limit_date
                  ,'count':count},
        dag=dag
    )
    check_review_start_notify >> review_crawling >> update_excute_date
    
    