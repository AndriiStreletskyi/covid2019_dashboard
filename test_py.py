import time
from datetime import date
from dateutil import parser
import pandas as pd
from pandas.core.frame import DataFrame
from global_params import QUE_UPDATE_FILE
from covid_data_handler import scheduler, check_outdated_data_que, covid_API_request, parse_csv_data, process_covid_csv_data, schedule_covid_updates, delete_from_data_que, joint_que, restore_data_que

def test_parse_csv_data():
    data = parse_csv_data("nation_2021-10-28.csv")
    print(type(data))
    assert len(data) == 639


def test_process_covid_csv_data():
    last7days_cases , current_hospital_cases , total_deaths = process_covid_csv_data("nation_2021-10-28.csv")
    assert last7days_cases == 240_299
    assert current_hospital_cases == 7_019
    assert total_deaths == 141_544


def test_covid_API_request():
    data = covid_API_request()
    assert isinstance(data, DataFrame) is True


def test_schedule_covid_updates():
    update_interval = "11:55"
    update_name = "test_que"
    schedule_covid_updates(update_interval, update_name)
    assert len(scheduler.queue)>0
    today = date.today()
    hole_date = parser.isoparse(str(today) + " " + update_interval)
    conv_date_tuple = hole_date.timetuple()
    interval = time.mktime(conv_date_tuple)
    now = time.time()
    if interval <= now:
        interval += 60*60*24
    assert scheduler.queue[0][0] == interval
    delete_from_data_que()


def test_check_outdated_data_que():
    check_outdated_data_que("test_data_update_que.csv","after_test_data_update_que.csv")
    assert len(pd.read_csv("test_data_update_que.csv",index_col="index")) != len(pd.read_csv("after_test_data_update_que.csv",index_col="index"))
    assert len(pd.read_csv("after_test_data_update_que.csv",index_col="index")) == 0


def test_joint_que():
    result = joint_que(0,"test_data_update_que.csv","after_test_data_update_que.csv")
    assert len(result) == len(pd.read_csv("test_data_update_que.csv",index_col="index")) + len(pd.read_csv("after_test_data_update_que.csv",index_col="index"))

def test_restore_data_que():
    restore_data_que("test_data_update_que.csv")
    assert len(pd.read_csv("test_data_update_que.csv",index_col="index")) == len(scheduler.queue)
    for elem in scheduler.queue:
        scheduler.cancel(elem)
    assert len(scheduler.queue) == 0


def test_delete_from_data_que():
    df = pd.read_csv("test_data_update_que.csv",index_col="index")
    original = len(df)
    restore_data_que("test_data_update_que.csv")
    delete_from_data_que(0, "test_data_update_que.csv")
    df_d = pd.read_csv("test_data_update_que.csv",index_col="index")
    assert (original-len(df_d)) == 1
    for elem in scheduler.queue:
        scheduler.cancel(elem)
    assert len(scheduler.queue) == 0

