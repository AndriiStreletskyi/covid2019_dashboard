import csv
import math
import sched
import time
from datetime import date
from dateutil import parser
import pandas as pd
from uk_covid19 import Cov19API
from logger import func_logger
from global_params import cases_and_deaths, QUE_UPDATE_FILE, NEWS_UPDATE_QUE, COVID_DATA, LOCATION, LOCATION_TYPE

scheduler = sched.scheduler(time.time,time.sleep)

@func_logger
def parse_csv_data(csv_filename):
    """Parse csv file and return list of rows"""
    with open(csv_filename,newline='') as file:
        reader = csv.reader(file)
        #next(reader)
        data = list(reader)
    return data

@func_logger
def process_covid_csv_data(covid_csv_data=COVID_DATA):
    """Process COVID19 data file into data. Returns: last7days_cases,current_hospital_cases,total_deaths"""
    last7days_cases = 0
    current_hospital_cases = 0
    total_deaths = 0
    i = 0
    parsing = parse_csv_data(covid_csv_data)
    covid_csv_data = parsing
    if len(covid_csv_data)>15:
        for i in range(3,10):
            if covid_csv_data[i][6] != "":
                last7days_cases += int(covid_csv_data[i][6])
        if covid_csv_data[1][5] != "":
            current_hospital_cases = int(covid_csv_data[1][5])
        if covid_csv_data[14][4] != "":
            total_deaths = int(covid_csv_data[14][4])
    return last7days_cases,current_hospital_cases,total_deaths

@func_logger
def covid_API_request(location=LOCATION,location_type=LOCATION_TYPE):
    """API request to get update of COVID data"""
    area_type = "areaType=" + location_type
    area_name = "areaName=" + location
    filter_conf = [area_type,area_name]

    api = Cov19API(filters=filter_conf, structure=cases_and_deaths)
    data = api.get_dataframe()
    if len(data)>1:
        data.to_csv(COVID_DATA, index=False)
        return data
    return "No data recived"

@func_logger
def schedule_covid_updates(update_interval, update_name, isRepeatable = False):
    """Creates an element in data que with given interval and name (can be Repeatable)"""
    # get todays date info
    today = date.today()
    # make an ISO datetype by joining date + provided interval - time, and parse it
    hole_date = parser.isoparse(str(today) + " " + update_interval)
    # make a time tuple for mktime function
    conv_date_tuple = hole_date.timetuple()
    # calculate needed interval from epoch, will be used for queing updates
    interval = time.mktime(conv_date_tuple)
    # get now from epoch
    now = time.time()

    if interval <= now:
        # if interval less than now, we move point in future for 24 hr
        interval += 60*60*24
        sched_obj = scheduler.enterabs(interval,1, covid_API_request, ())

    elif interval > now:
        sched_obj = scheduler.enterabs(interval,1, covid_API_request, ())

    # Open filename at QUE_UPDATE_FILE where the information about que is stored
    # and appned new line with data
    df = pd.read_csv(QUE_UPDATE_FILE,index_col="index").append({"content": update_interval,
                                            "title": update_name,
                                            "time": float(interval),
                                            "isRepeatable": str(isRepeatable)},
                                            ignore_index=True)

    # Sorting from less time period to max (as it will be in que object)
    df.sort_values(["time"], 
                    axis=0,
                    ascending=[True], 
                    inplace=True)
    df.reset_index(inplace=True,drop=True)
    df.to_csv(QUE_UPDATE_FILE, index=True, index_label="index")

    return sched_obj

@func_logger
def check_outdated_data_que(file_name_open=QUE_UPDATE_FILE, file_name_save=QUE_UPDATE_FILE):
    """Check if there is outdated requests in data que, if so delete them from que"""
    with open(file_name_open,"r") as file:
        reader = csv.reader(file)
        next(reader)
        df_s = list(reader)
        if len(df_s)>0:
            dataFrame = pd.DataFrame(df_s,columns=["index","content","title","time","isRepeatable"])
            dataFrame = dataFrame.drop(["index"],axis=1)
            now = time.time()
            s_dataFrame = dataFrame[dataFrame["time"].astype(float) > now]
            s1_df = dataFrame[dataFrame["isRepeatable"] == "True"]
            s1_df = s1_df[s1_df["time"].astype(float) < now]
            for index,row in s1_df.iterrows():
                row["time"] = float(row["time"])+float(math.ceil((now - float(row["time"])) / (60*60*24))) * (60*60*24)
            result = pd.concat([s_dataFrame, s1_df])
            result.sort_values(["time"],
                                axis=0,
                                ascending=[True],
                                inplace=True)
            result.reset_index(inplace=True,drop=True)
            result.to_csv(file_name_save, index=True, index_label="index")
            return "Check of outdated data complete"
        return "Nothing to check. File is empty"

@func_logger
def joint_que(return_df=0,first_que=QUE_UPDATE_FILE,second_que=NEWS_UPDATE_QUE):
    """Makes and returns 1 list of 2 que files to display in browseer"""
    df_d = pd.read_csv(first_que,index_col="index")
    df_n = pd.read_csv(second_que, index_col="index")

    if len(df_d)>0:
        df_d.insert(len(df_d.columns),"que_index",0)
        df_d.insert(len(df_d.columns),"type","data")
        for index,row in df_d.iterrows():
            df_d.at[index,"que_index"]=index
            df_d.at[index,"type"] = "data" + str(index)
    if len(df_n)>0:
        df_n.insert(len(df_n.columns),"que_index",0)
        df_n.insert(len(df_n.columns),"type","news")
        for index,row in df_n.iterrows():
            df_n.at[index,"que_index"]=index
            df_n.at[index,"type"] = "news" + str(index)
    if len(df_d)>0 or len(df_n)>0:
        updates = pd.concat([df_d, df_n])
        updates.sort_values(["time"],
                                    axis=0,
                                    ascending=[True],
                                    inplace=True)
        updates.reset_index(inplace=True,drop=True)
        result = updates.to_dict(orient="records")
        if (return_df == 1):
            return updates
        return result
    if len(df_d) == 0 and len(df_n) == 0:
        return []

@func_logger
def restore_data_que(filename=QUE_UPDATE_FILE):
    """Restores data que from file"""
    if scheduler.empty() is True:
        with open(filename,"r") as file:
            reader = csv.reader(file)
            next(reader)
            df_s = list(reader)
            if len(df_s)>0:
                for element in df_s:
                    scheduler.enterabs(float(element[3]),1, covid_API_request, ())
                return f"Restored {filename} que"
            return f"Data in {filename} is empty"
    else:
        return f"Current que for {filename} is not empty"

@func_logger
def delete_from_data_que(que_index=0, filename=QUE_UPDATE_FILE):
    """Removes element from data que and data que file using index"""
    if scheduler.empty() is not True and len(scheduler.queue) > que_index:
        # delete element from que
        scheduler.cancel(scheduler.queue[que_index])

        # delete que_index+1 row in csv file
        df = pd.read_csv(filename,index_col="index",skiprows=[que_index+1])
        df.reset_index(inplace=True,drop=True)
        df.to_csv(filename, index=True, index_label="index")
        check_outdated_data_que()
        return f"{filename} element with index {que_index} deleted"
    else:
        return "Delete is not possible"
