# crawling
import pandas as pd
import numpy
from selenium import webdriver
import re
import time
import csv
import pymysql

# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import pendulum
import requests

#--------------------------------실행 초기 설정 코드----------------------------------#

def get_musinsa_count(**kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:
            select_count = """
                SELECT count(*) from musinsa_shoes;
            """
            curs.execute(select_count)
            total = curs.fetchone()[0]
            
            select_musinsa_id = """
                SELECT musinsa_id
                  FROM musinsa_shoes;
            """
            curs.execute(select_musinsa_id)
            ids = curs.fetchall()
    finally:
        conn.close()

    counts = int(total / 500) + 1

    prod_ids_all = []
    for i in range(0, len(ids)):
        prod_ids_all.append(ids[i][0])
        
    splited_ids = []
    for count in range(0,counts):
        start_point = 500 * count
        end_point = 500 * (count + 1)
        try:
            prod_ids = prod_ids_all[start_point:end_point]
        except:
            prod_ids = prod_ids_all[start_point:]
        splited_ids.append(prod_ids)
        
    return counts, splited_ids
    
def date_check(**kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:
            sql = """
                SELECT * from lastcrawling;
            """
            curs.execute(sql)
            last_excute_date = curs.fetchone()[0]

    finally:
        conn.close()
    # 지난 실행일 전날의 23시59분59초 부터 이번 실행일 전날의 23시59분59초 까지의 리뷰를 수집
    limit_date = datetime.today() - timedelta(days=1)
    limit_date = limit_date.replace(hour=23,minute=59,second=59)
    last_excute_date = last_excute_date - timedelta(days=1)
    last_excute_date = last_excute_date.replace(hour=23,minute=59,second=59)
    return last_excute_date, limit_date
    
#--------------------------------크롤링 코드----------------------------------#

def get_shoes_review(prod_ids, last_excute_date, limit_date, **kwargs):

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)

    style_list = ['photo','goods']

    for style in style_list:
        for prod_id in prod_ids:
            try:
                with conn.cursor() as curs:
                    select_count = """
                        SELECT review_count from musinsa_shoes where musinsa_id=%s;
                    """
                    curs.execute(select_count, prod_id)
                    initial_count = curs.fetchone()[0]
            finally:
                conn.close()
            musinsa_reviews = []
            page_num = 0
            review_count = 0
            while True:
                page_num = page_num + 1
                url = 'https://store.musinsa.com/app/reviews/goods_estimate_list/'+str(style)+'/'+str(prod_id)+'/0/'+str(page_num)
                driver.get(url)
                time.sleep(1)
                driver.implicitly_wait(20)
                prod_rvw_date = driver.find_elements_by_class_name('date')
                #prod_name = driver.find_elements_by_class_name('list_info.p_name')
                #prod_cust_buy_size = driver.find_elements_by_class_name('txt_option')
                prod_size_jud = driver.find_elements_by_css_selector('body > div > div > div > div.postRight > div > div.prd-level-each > ul')
                prod_rvw = driver.find_elements_by_class_name('content-review')
                
                try:
                    no_data = driver.find_element_by_class_name('mypage_review_none')
                    if no_data != None:
                        break
                except:
                    pass
                    
                size = []
                footwidth = []
                ignition = []
                for prod_size_jud_split in prod_size_jud:
                    prod_size_jud_text = prod_size_jud_split.text
                    try:
                        test = prod_size_jud_text.split('\n')
                        size.append(test[0])
                        footwidth.append(test[3])
                        ignition.append(test[4])
                    except:
                        pass
                for q,r,si,fo,ig in zip(prod_rvw_date,prod_rvw,size,footwidth,ignition):
                    review_date = q.text.split(' ')[0]
                    check_date = datetime.strptime(review_date,'%Y.%m.%d')
                    if (last_excute_date < check_date) & (check_date < limit_date):
                        #buy_size = e.text
                        #buy_size = str(re.findall("2\d[0|5]",buy_size)[0])
                        #if buy_size == "":
                        #    buy_size = 0
                        review = r.text
                        review = review.replace('\n','')
                        
                        #신발 평가정보 피처링
                        if re.search('보', si) : si = 0
                        elif re.search('커', si) : si = 1
                        else : si = -1 
                        if re.search('보', fo) : fo = 0
                        elif re.search('넓', fo) : fo = 1
                        else : fo = -1 
                        if re.search('적', ig) : ig = 0
                        elif re.search('편', ig) : ig = 1
                        else : ig = -1 
                        
                        filename = '/home/reviews/musinsa.txt'
                        f = open(filename, 'a', encoding='utf-8', newline='')
                        f.write(f'{prod_id} {review_date[:10]} {si} {fo} {ig} {review}\n')
                        f.close()
                        
                        # 확인 및 백업을 위해 로컬에 csv파일로 저장
                        musinsa_reviews.append([prod_id, review_date, si, fo, ig, review])
            conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!', database='footfoot')
            try:
                with conn.cursor() as curs:
                    if initial_count < 20:
                        total_review = []
                        for info in musinsa_reviews:
                            total_review.append(info[5])
                        review4summary = '.'.join(total_review)
                        if (initial_count + review_count) > 19:
                            summarizer = Summarizer()
                            summary = '\n'.join(summarizer(review4summary))
                            set_summaries = """
                                UPDATE musinsa_shoes SET summaries=%s, review_count=%s WHERE musinsa_id=%s;
                            """
                            curs.execute(set_summaries, (summary, (initial_count + review_count), prod_id))
                        else:
                            get_tmp_review = """
                                SELECT tmp_review FROM musinsa_shoes WHERE musinsa_id=%s
                            """
                            curs.execute(get_tmp_review, (prod_id))
                            tmp_review = curs.fetchone()[0]
                            update_tmp= """
                                UPDATE musinsa_shoes SET tmp_review=%s, review_count=%s WHERE musinsa_id=%s;
                            """
                            curs.execute(update_tmp, ((review4summary + tmp_review), (initial_count + review_count), prod_id))
                    else:
                        update_count= """
                            UPDATE musinsa_shoes SET review_count=%s WHERE musinsa_id=%s;
                        """
                        curs.execute(update_count, ((initial_count + review_count), prod_id))
            finally:
                conn.close()    
            filename = f'/root/reviews/musinsa_{prod_id}_{style}.csv'
            with open(filename, 'w', encoding='utf-8', newline='') as f:
                csvwriter = csv.writer(f)
                csvwriter.writerow(['musinsa_id','review_date','size','foot','feel','review'])
                for i in musinsa_reviews:
                    csvwriter.writerow(i)
    driver.close()

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
        kwargs['ti'].xcom_push(key='musinsa_review_crawling_end', value=True)


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
      dag_id='musinsa_review_crawling_from_sql'
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
counts, splited_ids = get_musinsa_count()
last_excute_date, limit_date = date_check()

for count in range(0, counts):
    review_crawling = PythonOperator(
        task_id='{0}_review_crawling'.format(count),
        python_callable=get_shoes_review,
        op_kwargs={'prod_ids':splited_ids[count]
                  ,'last_excute_date':last_excute_date
                  ,'limit_date':limit_date},
        dag=dag
    )
    check_review_start_notify >> review_crawling >> update_excute_date

