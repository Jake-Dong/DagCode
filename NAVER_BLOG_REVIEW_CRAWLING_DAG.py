# crawling
import os
import sys
import urllib.request
import json
import requests
import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import time
import numpy as np
import pandas as pd
import re
import math
import random

# airflow 
from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import pendulum
import pymysql

#--------------------------------실행 초기 설정 코드----------------------------------#

def get_shoes_count(**kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')
    try:
        with conn.cursor() as curs:
            select_count = """
                SELECT count(*) from shoes;
            """
            curs.execute(select_count)
            total = curs.fetchone()[0]
            
            select_model_name = """
                SELECT brand, modelname
                  FROM shoes;
            """
            curs.execute(select_model_name)
            names = curs.fetchall()
            
    finally:
        conn.close()
        
    counts = int(total / 1000) + 1
    
    prod_ids_all = []
    for i in range(0, len(names)):
        prod_ids_all.append(names[i])
        
    splited_ids = []
    for count in range(0,counts):
        start_point = 1000 * count
        end_point = 1000 * (count + 1)
        try:
            prod_ids = prod_ids_all[start_point:end_point]
        except:
            prod_ids = prod_ids_all[start_point:]
        splited_ids.append(prod_ids)
        
    return counts, splited_ids

#--------------------------------크롤링 코드----------------------------------#

# 검색 가능한 페이지 수, 포스팅 수 분석(검색어, 한 페이지 당 결과 출력 수)
def get_blog_search_count(search_blog_keyword, display_count, client_id, client_secret, **kwargs):
    # 키워드에 사이즈 내용이 포함된 포스팅 검색어
    search_keyword = urllib.parse.quote(search_blog_keyword + " +사이즈")
    # json 결과
    url = "https://openapi.naver.com/v1/search/blog?query=" + search_keyword

    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id", client_id)
    request.add_header("X-Naver-Client-Secret", client_secret)
    response = urllib.request.urlopen(request)
    rescode = response.getcode()

    # 200 => 응답 성공 코드
    if (rescode == 200):
        response_body = response.read()
        # json으로 내용 받아오기
        response_body_dict = json.loads(response_body.decode('utf-8'))
        # 검색 결과 0개
        if response_body_dict['total'] == 0:
            # 검색 페이지 갯수
            blog_page_count = 0
            blog_page_total_count = 0

        # 검색결과가 있을 시
        else:
            # 페이지 수 = (총 검색 수 / 1회 출력 수) 반올림
            blog_page_total_count = math.ceil(response_body_dict['total'] / int(display_count))

            # 페이지 수가 100개 이상이면 페이지 수 = 100
            if blog_page_total_count >= 10:
                blog_page_count = 10
            # 페이지 수가 100개 미만이면 그대로
            else:
                blog_page_count = blog_page_total_count

        # 가능한 페이지 수 리턴
        return blog_page_count


# 리뷰 크롤링 함수(검색어, 한 페이지 당 결과 출력 수, 페이지 수)
def get_blog_post(search_blog_keyword, display_count, search_result_blog_page_count, client_id, client_secret, **kwargs):
    # 원본 url을 iframe 주소로 변환해서 담을 임시 리스트
    url_list = []
    # 리뷰 내용을 담을 리스트
    contents_list = []
    # 모든 내용을 담을 딕셔너리
    dicts = {}
    # 검색어
    encode_search_blog_keyword = urllib.parse.quote(search_blog_keyword + ' +사이즈')

    # 페이지 당 반복, url 크롤링
    for i in range(1, search_result_blog_page_count+1):
        try:
            if i == 1:
                url = "https://openapi.naver.com/v1/search/blog?query=" + encode_search_blog_keyword + "&display=" + str(
                    display_count) + "&start=" + str(i)
            elif i > 1:
                url = "https://openapi.naver.com/v1/search/blog?query=" + encode_search_blog_keyword + "&display=" + str(
                    display_count) + "&start=" + str(((i - 1) * display_count) + 1)

            request = urllib.request.Request(url)

            request.add_header("X-Naver-Client-Id", client_id)
            request.add_header("X-Naver-Client-Secret", client_secret)

            response = urllib.request.urlopen(request)
            response_code = response.getcode()

            if response_code == 200:
                response_body = response.read()
                response_body_dict = json.loads(response_body.decode('utf-8'))

                for j in range(0, len(response_body_dict['items'])):
                    try:
                        url_raw = response_body_dict['items'][j]['link']

                        if 'naver' in url_raw:
                            url_html = requests.get(url_raw)
                            url_text = url_html.text

                            url_soup = BeautifulSoup(url_text, 'html.parser')

                            iframe = url_soup.select_one('#mainFrame')
                            url = 'http://blog.naver.com' + iframe.get('src')
                            url_list.append(url)

                        else:
                            print('네이버 URL이 아닙니다.')
                            pass

                        print(i, j)

                    except:
                        pass

                # 랜덤 타임슬립
                time.sleep(random.uniform(2, 7))

            else:
                continue
        except:
            time.sleep(random.uniform(2, 4))
            break

    # 리뷰 크롤링
    for i in range(0, len(url_list)):
        try:
            url_data = url_list[i]

            real_url = requests.get(url_data)
            real_url_html = real_url.text

            real_url_soup = BeautifulSoup(real_url_html, 'html.parser')

            target_info = {}

            # 신 블로그
            if 'se_publishDate' in real_url_html:
                blog_title = real_url_soup.select_one('.pcol1').text
                # 제목

                blog_date = real_url_soup.select_one('.se_publishDate.pcol2').text
                # 날짜

                if 'se-main-container' in real_url_html:
                    # 신 블로그 1
                    blog_contents = real_url_soup.select('.se-main-container')
                    # 내용
                    content_list = []

                else:
                    # 신 블로그 2
                    blog_contents = real_url_soup.select('.se_component_wrap.sect_dsc.__se_component_area')
                    # 내용
                    content_list = []

            # 구 블로그
            else:
                blog_title = real_url_soup.select_one(".pcol1.itemSubjectBoldfont").text
                # 제목

                blog_date = real_url_soup.select_one(".date.fil5.pcol2._postAddDate").text
                # 날짜

                blog_contents = real_url_soup.select("#postViewArea")
                # 내용
                content_list = []

            for content in blog_contents:
                content_list.append(content.text)

            content_str = ' '.join(content_list)

            info_title = blog_title.strip()
            info_date = blog_date
            info_content = content_str.replace('\n', '')

            target_info['title'] = re.compile('[^ 0-9a-zA-Zㄱ-힗]').sub('', info_title)
            target_info['date'] = re.compile('[^ 0-9a-zA-Zㄱ-힗]').sub('', info_date)
            target_info['content'] = re.compile('[^ 0-9a-zA-Zㄱ-힗]').sub('', info_content)

            dicts[i] = target_info

        except:
            continue

    result_df = pd.DataFrame.from_dict(dicts, 'index')
    save_file_name = '_'.join(search_blog_keyword.split(' ')) # 리눅스에서 공백 파일명은 에러남
    result_df.to_csv(save_file_name + '_blog_Review.csv', encoding='utf-8')


def review_crawling_def(modelnames, client_id, client_secret, **kwargs):
    for blog_brand, shoes_keyword in modelnames:
        if not ((shoes_keyword == '') | (shoes_keyword == ' ')):
            search_blog_keyword = blog_brand + " " + shoes_keyword
            # 검색 가능한 페이지 수
            search_result_count = get_blog_search_count(search_blog_keyword, 100, client_id, client_secret)
            # URL + 리뷰 크롤링
            get_blog_post(search_blog_keyword, 100, search_result_count, client_id, client_secret)
            
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
        kwargs['ti'].xcom_push(key='naver_blog_crawling_end', value=True)

#--------------------------------에어 플로우 코드----------------------------------#

def check_review_start_notify(**kwargs):
    check = False
    while not check:
        check = kwargs['ti'].xcom_pull(key='review_crawling_start', dag_id='line_notify_review_crawling')
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
      dag_id='naver_blog_review_crawling_from_sql'
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

client_info = [
      ['I__kb5ol4fePl1HXXnQ5','UbUqlQ3axZ'],['xFG1dvdm1Sl4jt8BpVN0','puflFFlylt']
    , ['Lut8QTWuClANVW1KG1j','Xd92q3vFYi'],['7NHschBJB0ztLJTbhgrF','uH4BLvwpft']
    , ['EjgecVNLHY564tDBd8UJ','UCPJhexfCx'],['r5i6bGgzVYHkXsMqIwiC','BV6YkPWwPy']
]

# DAG 동적 생성
counts, modelnames = get_shoes_count()
info_n=0

for count in range(0, counts):
    if info_n == len(client_info):
        info_n=0
    name = modelnames[count]
    c_id = client_info[info_n][0]
    c_secret = client_info[info_n][1]
    review_crawling = PythonOperator(
        task_id='{0}_review_crawling'.format(count),
        python_callable=review_crawling_def,
        op_kwargs={'modelnames':name
                  ,'client_id':c_id
                  ,'client_secret':c_secret},
        dag=dag
    )
    info_n = info_n + 1
    check_review_start_notify >> review_crawling >> update_excute_date
