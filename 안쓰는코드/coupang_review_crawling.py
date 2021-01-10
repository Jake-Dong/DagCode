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

# 쿠팡 모델명, ID 뽑기
def get_shoes_info():

    brand_info = {
          'adidas' : '80'
        , 'nike' : '79'
        , 'fila' : '83'
        , 'puma' : '81'
        , 'newbalance' : '84'
        , 'reebok' : '85'
        , 'converse' : '86'
        , 'superga' : '1806'
    }

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)
    # 재익이
    # 크롤링한 신발들의 정보를 담을 리스트
    shoes_info = []
    
    for b_name, page in brand_info.items():
        for i in range(1, 10):
            url = 'https://www.coupang.com/np/categories/187365?listSize=120&brand='+page+'&offerCondition=&filterType=&isPriceRange=false&minPrice=&maxPrice=&page='+str(i)+'&channel=user&fromComponent=N&selectedPlpKeepFilter=&sorter=bestAsc&filter=&rating=0'
            driver.get(url)
            driver.implicitly_wait(4)
            prod_ids = driver.find_elements_by_xpath('/html/body/div[2]/section/form/div/div/div[1]/div/ul/li')
            prod_names = driver.find_elements_by_xpath('/html/body/div[2]/section/form/div/div/div[1]/div/ul/li/a/dl/dd/div[2]')
            for q, w in zip(prod_ids, prod_names):
                prod_id = q.get_attribute('id')
                prod_name = w.text
                shoes_info.append([b_name, prod_id, prod_name])
                
    now = dt.datetime.now()
    nowDate = now.strftime('%Y_%m_%d')
    filename = '/root/reviews/coupang_prod_id_{}.csv'.format(nowDate)
    f = open(filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow(['brand', 'prod_id', 'prod_name'])
    for i in shoes_info:
        csvWriter.writerow(i)
    f.close()
    driver.close()
    
def get_shoes_review():
    # prod_id 불러오기
    now = dt.datetime.now()
    nowDate = now.strftime('%Y_%m_%d')
    coupang_prod_id_path = '/root/reviews/coupang_prod_id_{}.csv'.format(nowDate)
    prod_dataframe = pd.read_csv(coupang_prod_id_path)
    prod_ids = prod_dataframe['prod_id']

    review_list = []
    info_list = []
    
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
            prod_review_date = driver.find_elements_by_class_name ('sdp-review__article__list__info__product-info__reg-date')
            prod_info = driver.find_elements_by_class_name     ('sdp-review__article__list__info__product-info__name')
            prod_cst_name = driver.find_elements_by_class_name ('sdp-review__article__list__info__user__name.js_reviewUserProfileImage')
            prod_size_jud = driver.find_elements_by_class_name ('sdp-review__article__list__survey')
            prod_review = driver.find_elements_by_class_name      ('sdp-review__article__list__review.js_reviewArticleContentContainer')
            prod_img = driver.find_elements_by_class_name       ('sdp-review__article__list__attachment__img.js_reviewArticleListImage.js_reviewArticleCrop')
            try:
                no_data = driver.find_element_by_css_selector    ('body > div.sdp-review__article__no-review.sdp-review__article__no-review--active')
                if no_data != None:
                    break
            except:
                pass
            # 텍스트 크롤링
            for prod_review_date_line \
                    , prod_info_line \
                    , prod_cst_name_line \
                    , prod_size_jud_line \
                    , prod_review_line in zip(prod_review_date, prod_info, prod_cst_name, prod_size_jud, prod_review):
                prod_review_date_text = prod_review_date_line.text
                prod_info_split_text = prod_info_line.text
                prod_cst_name_text = prod_cst_name_line.text
                prod_size_jud_split_text = prod_size_jud_line.text
                prod_review_text = prod_review_line.text
                try:
                    prod_info_split = prod_info_split_text.split(',')
                    prod_name = prod_info_split[0].replace(' ', '')
                    prod_color = prod_info_split[1]
                    prod_by_size = prod_info_split[2]
                    review_list.append([prod_review_date_text
                                        , prod_cst_name_text
                                        , prod_name
                                        , prod_color
                                        , prod_by_size
                                        , prod_review_text])
                    prod_size_jud_split = prod_size_jud_split_text.split('\n')
                    cst_size = prod_size_jud_split[0]
                    cst_foot_width = prod_size_jud_split[1]
                    cst_size_jud = prod_size_jud_split[2]
                    info_list.append([prod_review_date_text
                                         , prod_cst_name_text
                                         , cst_size
                                         , cst_foot_width
                                         , cst_size_jud])
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
            progress_notify = f'쿠팡 리뷰 크롤링 {str(progress_percent)}% 완료되었습니다.'
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
    # 텍스트 저장
    now = dt.datetime.now()
    nowDate = now.strftime('%Y_%m_%d')
    review_filename ='/root/reviews/coupang_reviews_{}.csv'.format(nowDate)
    cst_filename ='/root/reviews/coupang_cst_{}.csv'.format(nowDate)

    f = open(review_filename, 'w', encoding='utf-8', newline='')
    j = open(cst_filename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriters = csv.writer(j)

    csvWriter.writerow(['prod_review_date','prod_cst_name','prod_name','prod_color','prod_by_size','prod_review'])
    for q in review_list:
        csvWriter.writerow(q)

    csvWriters.writerow(['prod_review_date','prod_cst_name','cst_size','cst_foot_width','cst_size_jud'])
    for w in info_list:
        csvWriters.writerow(w)
    f.close()
    j.close()


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
# id 크롤링
id_crawling_code = PythonOperator(
    task_id='id_crawling',
    python_callable=get_shoes_info,
    queue='q22',
    dag=dag
)
# 리뷰 크롤링
review_crawling_code = PythonOperator(
    task_id='review_crawling',
    python_callable=get_shoes_review,
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
start_notify >> id_crawling_code >> review_crawling_code >> end_notify
'''