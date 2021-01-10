'''
# crawling
import pandas as pd
import numpy
from selenium import webdriver
import re
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
import datetime as dt
import time
import csv
# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import pendulum
import requests

   
def get_shoes_img():
    # prod_id 불러오기
    now = dt.datetime.now()
    nowDate = now.strftime('%Y_%m_%d')
    coupang_prod_id_path = '/root/reviews/coupang_prod_id_{}.csv'.format(nowDate)
    prod_dataframe = pd.read_csv(coupang_prod_id_path)
    prod_ids = prod_dataframe['prod_id']

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    
    progress = 0.0
    progress_check = 0
    # 모델별 크롤링 코드
    for prod_id in prod_ids:
        img_url_list = []
        page = 0
        while True:
            page = page + 1
            url = 'https://www.coupang.com/vp/product/reviews?productId=' + str(prod_id) + '&page=' + str(page) + '&size=100&sortBy=ORDER_SCORE_ASC&ratings=&q=&viRoleCode=2&ratingSummary=true'
            driver.get(url)
            time.sleep(3)
            prod_img = driver.find_elements_by_class_name('sdp-review__article__list__attachment__img.js_reviewArticleListImage.js_reviewArticleCrop')
            try:
                no_data = driver.find_element_by_css_selector('body > div.sdp-review__article__no-review.sdp-review__article__no-review--active')
                if no_data != None:
                    break
            except:
                pass
          
            # 이미지 크롤링
            for img_src in prod_img:
                review_img_URL = img_src.get_attribute('src')
                img_url_list.append(review_img_URL)
        # 이미지 저장
        n = 0
        for url in img_url_list:
            n = n + 1
            r = requests.get(url)
            file = open(f'/root/images/coupang_{prod_id}_{n}.jpg', 'wb')
            file.write(r.content)
            file.close()
            
        # 진행상황 체크                
        progress = progress + 1
        progress_percent = round((progress * 100) / float(len(prod_ids)))
        if progress_check < progress_percent :
            progress_check = progress_percent
            progress_notify = f'쿠팡 이미지 크롤링 {str(progress_percent)}% 완료되었습니다.'
            TARGET_URL = 'https://notify-api.line.me/api/notify'
            TOKEN = 'sw0dTqnM0kEiJETNz2aukiTjhzsrIQlmdR0gdbDeSK3'

            # 요청합니다.
            requests.post(
                TARGET_URL
                , headers={
                    'Authorization' : 'Bearer ' + TOKEN
                }
                , data={
                    'message' : progress_notify
                }
            )
    
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
    'start_date': datetime(2020, 9, 30, tzinfo=local_tz),
    'catchup': False,
    'retries': 1,
    'retry_delay':timedelta(minutes=1)
}    
    
# DAG인스턴스 생성
dag = DAG(
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='coupang_review_crawling'
    # DAG 설정을 넣어줌
    , default_args=default_args
    # 최대 실행 횟수
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(days=7)
)
# 크롤링 시작 알림
start_notify = PythonOperator(
    task_id='start_notify',
    python_callable=notify,
    op_kwargs={'context':'쿠팡 크롤링을 시작하였습니다.'},
    queue='q22',
    dag=dag

)
# 이미지 크롤링
review_crawling_code = PythonOperator(
    task_id='review_crawling',
    python_callable=get_shoes_img,
    queue='q22',
    dag=dag
)
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'쿠팡 크롤링이 종료되었습니다.'},
    queue='q22',
    dag=dag
)

# 실행 순서 설정
start_notify >> review_crawling_code >> end_notify
'''
