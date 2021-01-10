# airflow 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import time
import pendulum
import pymysql

def id_merge_update(**kwargs):
    conn = pymysql.connect(host='35.185.210.97', port=3306, user='footfootbig', password='footbigmaria!',
                           database='footfoot')

    try:
        with conn.cursor() as curs:
            sql = """
                truncate table shoes;
            """
            curs.execute(sql)
            
            sql2 = """
                insert ignore into shoes(brand, shono, modelname, category, shosex, price_m
                , price_d, minsize, maxsize, sizeunit, heelsize, musinsa_id, danawa_id)
                (select m.brand, m.shono, m.modelname, m.category, m.shosex, m.price_m
                , d.price_d, m.minsize, m.maxsize, m.sizeunit, d.heelsize, m.musinsa_id, d.danawa_id
                  from musinsa_shoes as m
                left outer join danawa_shoes as d
                on m.shono=d.shono and m.brand=d.brand)
                UNION
                (select d.brand, d.shono, d.modelname, d.category, d.shosex, m.price_m, d.price_d
                , m.minsize, m.maxsize, m.sizeunit, d.heelsize, m.musinsa_id, d.danawa_id
                  from musinsa_shoes as m
                right outer join danawa_shoes as d
                    on m.shono=d.shono and m.brand=d.brand);
            """
            curs.execute(sql2)
            conn.commit()

    finally:
        conn.close()
        kwargs['ti'].xcom_push(key='id_merge_update_end', value=True)
        
def check_id_crawling_end(**kwargs):
    check_danawa = False
    check_musinsa = False
    while not check_danawa:
        try:
            check_danawa = kwargs['ti'].xcom_pull(key='danawa_id_crawling_end', dag_id='danawa_id_crawling_to_sql')
        except:
            pass
        if not check_danawa:
            time.sleep(60*5)
    while not check_musinsa:
        try:
            check_musinsa = kwargs['ti'].xcom_pull(key='musinsa_id_crawling_end', dag_id='musinsa_id_crawling_to_sql')
        except:
            pass
        if not check_musinsa:
            time.sleep(60*5)

# 서울 시간 기준으로 변경
local_tz = pendulum.timezone('Asia/Seoul')
today = datetime.today()
# airflow DAG설정        
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(today.year, today.month, today.day, tzinfo=local_tz) - timedelta(days=15),
    'catchup': False,
    'provide_context': True
}    

# DAG인스턴스 생성
dag = DAG(
    # 웹 UI에서 표기되며 전체 DAG의 ID
      dag_id='id_merge_update'
    # DAG 설정을 넣어줌
    , default_args=default_args
    # 최대 실행 횟수
    , max_active_runs=1
    # 실행 주기
    , schedule_interval=timedelta(days=14)
)

id_merge_update = PythonOperator(
    task_id = 'id_merge_update',
    python_callable = id_merge_update,
    dag = dag,
)

# id 크롤링 실행 감지
check_id_crawling_end = PythonOperator(
    task_id = 'check_id_crawling_end',
    python_callable = check_id_crawling_end,
    dag = dag,
)

check_id_crawling_end >> id_merge_update

