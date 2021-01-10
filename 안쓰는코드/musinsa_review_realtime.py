'''
# crawling
import pandas as pd
import numpy as np
import re
import time
import csv
import datetime as dt
from bs4 import BeautifulSoup

# airflow 
from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import pendulum
import requests


category_info = {
      '구두' : '005014'
    , '부츠' : '005011'
    , '로퍼' : '005015'
    , '샌들' : '005004'
    , '슬리퍼' : '005018'
}
category_info_split = {
      '캔버스' : '018002'
    , '러닝화' : '018003'
    , '힐' : '005012'
    , '플랫' : '005017'
    , '스니커즈' : '018004'
}


def get_shoes_review(category, **kwargs):
#     now = dt.datetime.now()
    prod_id_csv = pd.read_csv('/root/reviews/musinsa_{}_id.csv'.format(category))
    prod_ids = prod_id_csv['musinsa_id']

    style_list = ['photo','goods']

    for style in style_list:
        musinsa_rvw_list = []
        
        for prod_id in prod_ids:
            page_num = 0

            while True:
                page_num = page_num + 1
                now = dt.datetime.now()
                
                url = 'https://store.musinsa.com/app/reviews/goods_estimate_list/'+str(style)+'/'+str(prod_id)+'/0/'+str(page_num)
                re_url = requests.get(url)
                re_url_html = re_url.text

                soup = BeautifulSoup(re_url_html, 'html.parser')

                prod_rvw_date = soup.select('.date')
                prod_cust_buy_size = soup.select('.txt_option')
                prod_size_jud = soup.select('.prd-level-each')
                prod_rvw = soup.select('.content-review')

                #모델이름
                try:
                    no_data = soup.select('.mypage_review_none')
                    if no_data != None:
                        pass

                except:
                    pass

                for prod_size_jud_split in prod_size_jud:
                    prod_size_jud_text = prod_size_jud_split.text
                    try:
                        test = prod_size_jud_text.strip().split('\n')
                        size = test[0]
                        footwidth = test[3]
                        ignition = test[4]

                    except:
                        pass
                for q,e,r in zip(prod_rvw_date,prod_cust_buy_size,prod_rvw):
                    musinsa_rvw_list.append\
                    ([q.text.replace('\t', '').replace('\n', ''), prod_id, e.text.replace('\t', '').replace('\n', ''), \
                      size, footwidth, ignition, r.text.replace('\t', '').replace('\n', '')])


        filename = f'/root/reviews/musinsa_{style}_{category}_reviews.csv'
        f = open(filename, 'w', encoding='utf-8', newline='')
        csvWriter = csv.writer(f)
        csvWriter.writerow(['review_date','musinsa_id','buy_size','sizefeel','footwidthfeel','feeling','review'])
        for w in musinsa_rvw_list:
            csvWriter.writerow(w)
        f.close()


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
      dag_id='musinsa_review_crawling'
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
    op_kwargs={'context':'무신사 리뷰 크롤링을 시작하였습니다.'},
    queue='qmaria',
    dag=dag
)
# 크롤링 종료 알림
end_notify = PythonOperator(
    task_id='end_notify',
    python_callable=notify,
    op_kwargs={'context':'무신사 리뷰 크롤링이 종료되었습니다.'},
    queue='qmaria',
    dag=dag
)
# id 크롤링 종료 감지
sensor = ExternalTaskSensor(
      task_id='external_sensor'
    , external_dag_id='musinsa_id_crawling'
    , external_task_id='end_notify'
    , mode='reschedule'
    , queue='qmaria'
    , dag=dag
)
# DAG 동적 생성
for name, page in category_info_split.items():
    # 크롤링 DAG
    review_crawling = PythonOperator(
        task_id='{0}_review_crawling'.format(page),
        python_callable=get_shoes_review,
        op_kwargs={'category':name},
        queue='qmaria',
        dag=dag
    )
    sensor >> start_notify >> review_crawling >> end_notify
    
# DAG 동적 생성
for name, page in category_info.items():
    # 크롤링 DAG
    review_crawling = PythonOperator(
        task_id='{0}_review_crawling'.format(page),
        python_callable=get_shoes_review,
        op_kwargs={'category':name},
        queue='q22',
        dag=dag
    )
    sensor >> start_notify >> review_crawling >> end_notify        
        
        
#                 date_str = str(prod_rvw_date.text)
                
#                 date_time_obj = datetime.datetime.strptime(date_str, '%Y.%m.%d %H:%M')

#                 if date_time_obj > now:
#                     print('크다')

#                 elif date_time_obj < now:
#                     print('작다')

#                 else:
#                     print('오류')
'''