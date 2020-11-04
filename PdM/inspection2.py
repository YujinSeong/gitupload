import logging
import pyodbc
from moving_ave import CurrDetector
import numpy as np
import pandas as pd
from datetime import timedelta

# logging instance 생성
logger = logging.getLogger(__name__)

# formatter 생성 (stream, file)
formatter = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s')

logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("PdM.log")
stream_handler = logging.StreamHandler()

file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def pdm(tool_cnt, curr, sum_q):
    """
    CurrDetector 클래스의 moving_ave 이용해 전류값 계산
    :param tool_cnt: 툴 사용 횟수
    :param curr: 전류 데이터
    :param sum_q: 이전까지 누적 전류 합
    :return: max_curr-전류 max(quantile 0.99), new_sum_q-누적 전류 합, moving_ave_curr-누적 전류 합/툴 사용 횟수
    """
    try:
        logger.info("Caculate Start")
        curr_info = CurrDetector()
        max_curr, new_sum_q, moving_ave_curr = curr_info.moving_ave(tool_cnt, curr, sum_q)
    except Exception as e:
        logger.exception(e)
    else:
        logger.info("pdm reault - max_curr:{}, sum_q:{}, new_sum_q:{}, moving_ave_curr:{}".format(max_curr, sum_q, new_sum_q, moving_ave_curr))
        return max_curr, new_sum_q, moving_ave_curr
    finally:
        curr_info.disp()
        logger.info("Caculate Done")


def get_data_from_db():
    """
    ESS_PDM_CURRENT에 접속해서 시간, 시리얼넘버, 툴사용횟수, 전류 데이터 받아오고
    ESS_PDM_RESULT에 접속해서 이전까지 누적 전류 합 받아옴
    :return: date, serial, tool_cnt, current_data, pre_sum_current_sql
    """
    flag = 1
    try:
        logger.info("DB Connect")

        conn = pyodbc.connect(
            "Driver={SQL Server Native Client 11.0};Server=172.20.56.220;Database=NEXPOM;Uid=essmgr;Pwd=essmgr;")

        cursor = conn.cursor()

        cursor.execute('''SELECT TOP 2 JOB_DATE from dbo.ESS_PDM_CURRENT ORDER BY JOB_DATE desc''')
        prev_time_sql = pd.DataFrame(cursor.fetchall())
        prev_time = prev_time_sql.iloc[1, 0]
        prev_time_later = prev_time[0] + timedelta(seconds=1)
        prev_time_later = str(prev_time_later)
        cursor.execute(
            '''SELECT TOP 1 JOB_DATE, SERIAL_NO, TOOL_CNT, CURRENT_DATA from dbo.ESS_PDM_CURRENT where FLAG =0 and JOB_DATE > ('%s') ORDER BY JOB_DATE desc;''' % (
            prev_time_later[:-7]))
        sql_val = cursor.fetchall()
        cursor.execute('''SELECT TOP 1 SUM_CURRENT from dbo.ESS_PDM_RESULT ORDER BY JOB_DATE desc''')
        pre_sum_q = cursor.fetchall()

    except Exception as e:
        logger.info("DB Connect fail")
        logger.exception(e)
    else:
        logger.info("DB Connect success")
        if len(sql_val) == 0:
            date, serial, tool_cnt, current_data= 0,0,0,0
        else:
            sql_val = pd.DataFrame(np.array(sql_val), columns=['JOB_DATE', 'SERIAL_NO', 'TOOL_CNT', 'CURRENT_DATA'])
            date = sql_val.iloc[0, 0]
            serial = sql_val.iloc[0, 1]
            tool_cnt = sql_val.iloc[0, 2]
            current_data = np.array(sql_val.iloc[0, 3].split(';'), dtype=np.float64)
        if pre_sum_q == []:
            pre_sum_q = 0
        else : pre_sum_q = np.array(pre_sum_q[0][0], dtype=np.float64)

        cursor.execute(
            '''UPDATE ESS_PDM_CURRENT SET FLAG = ('%d') where SERIAL_NO = ('%s') and TOOL_CNT = ('%d') and FLAG = 0;''' % (
                flag, serial, tool_cnt))
        conn.commit()
        logger.info("flag change done")

        conn.close()
        logger.info(
            "get data - date:{}, serial:{}, tool_cnt:{}".format(date, serial, tool_cnt))

        return date, serial, tool_cnt, current_data, pre_sum_q


def upload_data_to_db(serial, tool_cnt, max_curr, new_sum_q, moving_ave_curr):
    """
    ESS_PDM_RESULT 에 접속해서 ESS_PDM_CURRENT에서 받아온 serial, tool_cnt, pdm 결과 데이터 업로드
    :param serial: 제품 시리얼
    :param tool_cnt: 툴 사용 횟수
    :param max_curr: 전류 max(quantile 0.99)
    :param new_sum_q: 이전 누적 전류 합 + 전류 max 값
    :param moving_ave_curr: 전류 평균
    :return: none
    """
    try:
        conn = pyodbc.connect(
            "Driver={SQL Server Native Client 11.0};Server=172.20.56.220;Database=NEXPOM;Uid=essmgr;Pwd=essmgr;")
        cursor = conn.cursor()
        logger.info("Data Upload..")
        cursor.execute('''INSERT ESS_PDM_RESULT(JOB_DATE, WC,OP_NO, MCH_CODE, SERIAL_NO, TOOL_CNT, MAX_CURRENT, 
        SUM_CURRENT, MOVING_AVG_CURRENT, AVG_PRESSURE,STD_PRESSURE, Z_VALUE, MINUS_TREND, PLUS_TREND, FLAG) VALUES(
        GETDATE(), 'CEPA34',  '240-1', '01', '%s', '%d', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);''' % (
            serial, tool_cnt))
        conn.commit()

        cursor.execute(
            '''UPDATE ESS_PDM_RESULT SET MAX_CURRENT = ('%s'), SUM_CURRENT = ('%s'), MOVING_AVG_CURRENT=('%s') where SERIAL_NO = ('%s') and TOOL_CNT = ('%d');''' % (
                max_curr, new_sum_q, moving_ave_curr, serial, tool_cnt))
        conn.commit()
    except Exception as e:
        logger.info("Upload Fail")
        logger.exception(e)
    else:
        logger.info("Upload Done")
        conn.close()


def main():
    """
    메인 실행함수
    :return: none
    """
    try:
        date, serial, tool_cnt, current_data, pre_sum_q = get_data_from_db()
        if date == 0:
            raise ValueError("No new data")
        max_curr, new_sum_q, moving_ave_curr = pdm(tool_cnt, current_data, pre_sum_q)
        upload_data_to_db(serial, tool_cnt, max_curr, new_sum_q, moving_ave_curr)
    except Exception as e:
        logger.exception(e)
    finally:
        print("실행완료")


if __name__ == '__main__': main()
