'''
import os
import sys
import urllib.request
import json
import requests
import urllib.request
import urllib.error
import urllib.parse
from bs4 import BeautifulSoup
import time
import numpy as np
import pandas as pd
import re
import math
import random
import traceback

client_id = "I__kb5ol4fePl1HXXnQ5"
client_secret = "UbUqlQ3axZ"


# client_id = "xFG1dvdm1Sl4jt8BpVN0"
# client_secret = "puflFFlylt"
# client_id = "Lut8QTWuClANVW1KG1j"
# client_secret = "Xd92q3vFYi"
# client_id = "7NHschBJB0ztLJTbhgrF"
# client_secret = "uH4BLvwpft"
# client_id = "EjgecVNLHY564tDBd8UJ"
# client_secret = "UCPJhexfCx"
# client_id = "r5i6bGgzVYHkXsMqIwiC"
# client_secret = "BV6YkPWwPy"


# 블로그 전체 실행(검색어, 한 페이지 당 결과 출력 수)
def naver_blog_crawling(search_blog_keyword, display_count):
    # 검색 가능한 페이지 수
    search_result_blog_page_count = get_blog_search_result_page_count(search_blog_keyword, display_count)
    # URL + 리뷰 크롤링
    get_blog_post(search_blog_keyword, display_count, search_result_blog_page_count)


# 검색 가능한 페이지 수, 포스팅 수 분석(검색어, 한 페이지 당 결과 출력 수)
def get_blog_search_result_page_count(search_blog_keyword, display_count):
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

        print("키워드 " + search_blog_keyword + " 총 포스팅 수 : " + str(response_body_dict['total']))
        print("키워드 " + search_blog_keyword + " 실제 페이지 수 : " + str(blog_page_total_count/100))
        print("키워드 " + search_blog_keyword + " 진행 가능한 페이지 수 : " + str(blog_page_count))

        # 가능한 페이지 수 리턴
        return blog_page_count


# 리뷰 크롤링 함수(검색어, 한 페이지 당 결과 출력 수, 페이지 수)
def get_blog_post(search_blog_keyword, display_count, search_result_blog_page_count):
    # 원본 url을 iframe 주소로 변환해서 담을 임시 리스트

    re_img = re.compile('postfiles.pstatic.net')
    
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
                        traceback.print_exc()

                # 랜덤 타임슬립
                time.sleep(random.uniform(2, 7))

            else:
                print('Api 호출 에러 >> 재시도')
                traceback.print_exc()
                continue
        except:
            print('반복 >> 재시도')
            traceback.print_exc()
            time.sleep(random.uniform(2, 4))
            break

    # 리뷰 크롤링
    for i in range(0, len(url_list)):
        try:
            url_data = url_list[i]

            real_url = requests.get(url_data)
            real_url_html = real_url.text

            real_url_soup = BeautifulSoup(real_url_html, 'html.parser')

            blog_img = real_url_soup.find_all(attrs={'class':'se_mediaImage __se_img_el'})
            
            img_list = []
            
            if '_photoImage' in real_url_html:
                blog_img = real_url_soup.find_all(attrs={'class':'_photoImage'})
                img_list = []

                for img in blog_img:
                    img_data = img.get('src')
                    img_list.append(img_data)
#                     print(img_list)
            
            elif 'se_mediaImage __se_img_el' in real_url_html:
                blog_img = real_url_soup.find_all(attrs={'class':'se_mediaImage __se_img_el'})            
                img_list = []
                
                for img in blog_img:
                    img_data = img.get('src')
                    img_list.append(img_data)
#                     print(img_list)

            elif 'se-image-resource' in real_url_html:
                blog_img = real_url_soup.find_all(attrs={'class':'se-image-resource'})
                img_list = []
            
            else:
                pass
            
            for img in blog_img:
                img_data = img.get('src')
                img_list.append(img_data)

                
        except:
            print('리뷰 크롤링 Error')
            traceback.print_exc()
            continue

        if not os.path.isdir('./{}'.format(search_blog_keyword)):
            os.mkdir('./{}'.format(search_blog_keyword))
            n = 0
            for img_url in img_list:
                n = n + 1
                r = requests.get(img_url)
                file = open(r'./{0}/{1}.jpg'.format(search_blog_keyword, (search_blog_keyword + str(i) + '_' + str(n))),'wb')
                file.write(r.content)
                file.close()

        else:
            n = 0
            for img_url in img_list:
                n = n + 1
                r = requests.get(img_url)
                file = open(r'./{0}/{1}.jpg'.format(search_blog_keyword, (search_blog_keyword +  str(i) + '_' + str(n))),'wb')
                file.write(r.content)
                file.close()



def review_crawling():
    startTime = time.time()
    name_data = pd.read_csv('./danawa_반스_id.csv', encoding='utf-8')
    blog_brand = name_data['brand'][0]
    shoes_name = name_data['modelname']

    for shoes_keyword in shoes_name:
        search_blog_keyword = blog_brand + " " + shoes_keyword
        naver_blog_crawling(search_blog_keyword, 100)
'''
    endTime = time.time() - startTime