'''
# crawling
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import re
import time
import csv
import pandas as pd
import numpy as np
import datetime as dt

# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import sys
import pendulum
import requests

brand_info = {
      '골든구스' : '10697', '반스' : '10720', '라코스테' : '6559', '돔바' : '13645', '컨버스' : '10986'
    , '프레드페리' : '10601', '메종마르지엘라' : '35048', '버버리' : '10562', '락포트' : '10821', '알렉산더맥퀸' : '14288'
    , '탠디' : '10812', '엘칸토' : '10859', '리차드' : '6642', '발렌시아가' : '10803', '소다' : '6953'
    , '발렌티노' : '10741', 'MLB' : '10579', '오니츠카타이거' : '10388857', '구찌' : '10794', '닥스' : '44805'
    , '제옥스' : '10913' , '엑셀시오르' : '27451', '프리웨이' : '6641', '아떼바네사브루노' : '42907' , '스타지오네바이엘칸토' : '36625'
    , '폴스미스' : '10462', '생로랑파리' : '10756', '크록스' : '10828', '슈펜' : '30033', '미소페' : '10698'
    , '프라다' : '10561' , '지방시' : '10735', '핏플랍' : '10867', '영에이지' : '14582092', '플로쥬' : '29793216'
    , '호킨스' : '10719', '나이키' : '13876', '스티유' : '13922', '리복' : '13770', '아멜리에' : '27810'
    , '푸마' : '12042', '프로스펙스' : '25922412', '아식스' : '6345', '디스커버리익스페디션' : '29957', '르까프' : '27161'
    , '스케쳐스' : '13949', '미즈노' : '31561', '월드컵' : '26402', '노스페이스' : '29956', '브룩스' : '10405600'
}
brand_info_split = {
      '요넥스' : '13806', '르꼬끄' : '5248', '슬레진저' : '18865', '호카오네오네' : '25462089', '언더아머' : '31563'
    , '카파' : '13997', '데상트' : '11764', '맥스' : '34861', '케이스위스' : '11028', '네파' : '13755'
    , '가데옴므' : '35419', '포트폴리오' : '14582122', '와키앤타키' : '6680', '리갈' : '6339', '에스콰이아' : '14582020'
    , '토즈' : '10891', '제니아에센셜' : '20642094', '레노마' : '10537', '제니아' : '20642088', '보닌' : '7061'
    , '레페토' : '10866', '엘리자벳' : '10822', '페라가모' : '10090', '잭앤질슈즈' : '11193', '가이거' : '11101'
    , '심플리트' : '14582323', '세라' : '10831', '고세' : '10824', '스퍼' : '6363', '포멜카멜레' : '35961'
    , '바바라' : '6364', '메쎄' : '10912', '레이첼콕스' : '10840' , '베카치노' : '9519', '토리버치' : '10773'
    , '버켄스탁' : '10935', '페이퍼플레인' : '35422', '슈스파' : '10430', '테바' : '14156', 'SNRD' : '35423'
    , '닥터마틴' : '10747', '팀버랜드' : '10942', '무다' : '35421', '알도' : '13911', '쏘로굿' : '10749'
    , '아디다스' : '10851', '수페르가' : '10750', '뉴발란스' : '13760', '라그라치아' : '11681188', '휠라' : '10789'
}

def get_shoes_img(b_name, **kwargs):

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    # 드라이버 위치도 맞출 것
    driver = webdriver.Chrome('드라이버 위치',options=options)

    # prod_id 불러오기 - 자신의 환경에 맞는 위치로 설정할것
    danawa_prod_id_path = r'/root/reviews/danawa_{}_id.csv'.format(b_name)
    prod_dataframe = pd.read_csv(danawa_prod_id_path)
    prod_ids = prod_dataframe['danawa_id']

    for prod_id in prod_ids:
        img_url_list = []
        page = 0
        while True:
            page = page + 1
            url = 'http://prod.danawa.com/info/dpg/ajax/companyProductReview.ajax.php?t=0.10499996477784657&prodCode='+str(prod_id)+'&cate1Code=1824&page='+str(page)+'&limit=100&score=0&sortType=&usefullScore=Y&innerKeyword=&subjectWord=0&subjectWordString=&subjectSimilarWordString=&_=1600608005961'
            driver.get(url)
            time.sleep(3)
            danawa_img_list = driver.find_elements_by_class_name('center > img')
            for img_src in danawa_img_list:
                danawa_img_url = img_src.get_attribute('src')
                img_url_list.append(danawa_img_url)
                
        n = 0
        for url in img_url_list:
            n = n + 1
            r = requests.get(url)
            file = open(r'/root/img/danawa_{0}_{1}.jpg'.format(prod_id, n), 'wb')
            file.write(r.content)
            file.close()
            

    driver.close()

# 입력받은 context를 라인으로 메시지 보내는 함수
def notify(context, **kwargs): 
    TARGET_URL = 'https://notify-api.line.me/api/notify'
    TOKEN = 'sw0dTqnM0kEiJETNz2aukiTjhzsrIQlmdR0gdbDeSK3'

    # 요청합니다.
    requests.post(
        TARGET_URL
        , headers={
            'Authorization' : 'Bearer ' + TOKEN
        }
        , data={
            'message' : context
        }
    )

# 서울 시간 기준으로 변경
local_tz = pendulum.timezone('Asia/Seoul')

# airflow DAG설정        
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 1, tzinfo=local_tz),
    'catchup': False,
    'retries': 2,
    'retry_delay':timedelta(minutes=1)
}    
    
# DAG인스턴스 생성
dag = DAG(
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='danawa_img_crawling'
    # DAG 설정을 넣어줌
    , default_args=default_args
    # 최대 실행 횟수
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(minutes=1)
)
# 크롤링 시작 알림
start_notify = PythonOperator(
    task_id='start_notify',
    python_callable=notify,
    op_kwargs={'context':'다나와 이미지 크롤링을 시작하였습니다.'},
    queue='qmaria',
    dag=dag
)
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'다나와 이미지 크롤링이 종료되었습니다.'},
    queue='qmaria',
    dag=dag
)

# DAG 동적 생성
for b_name, page in brand_info.items():
    # 크롤링 DAG
    review_crawling = PythonOperator(
        task_id='{0}_img_crawling'.format(page),
        python_callable=get_shoes_img,
        op_kwargs={'b_name':b_name},
        queue='q22',
        dag=dag
    )
    start_notify >> review_crawling >> end_notify
    
# DAG 동적 생성
for b_name, page in brand_info_split.items():
    # 크롤링 DAG
    review_crawling = PythonOperator(
        task_id='{0}_img_crawling'.format(page),
        python_callable=get_shoes_img,
        op_kwargs={'b_name':b_name},
        queue='qmaria',
        dag=dag
    )
    start_notify >> review_crawling >> end_notify
'''