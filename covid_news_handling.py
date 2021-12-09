import sched
import time
import csv
from datetime import date
from dateutil import parser
from newsapi import NewsApiClient
from numpy import fabs
import pandas as pd
from global_params import API_KEY, NEWS_DATA, NEWS_UPDATE_QUE
from logger import func_logger
from covid_data_handler import check_outdated_data_que, restore_data_que, delete_from_data_que

api = NewsApiClient(api_key = API_KEY)
news_scheduler = sched.scheduler(time.time,time.sleep)

@func_logger
def news_API_request(covid_terms = "Covid COVID-19 coronavirus"):
    """Get news from API request. Write only new content to file"""
    data = api.get_everything(q = covid_terms)

    df = []

    for article in data["articles"]:

        with open(NEWS_DATA,"r") as file:
            reader = csv.reader(file)
            next(reader)
            df = list(reader)
            file.close()

        if len(df) > 0:
            
            overlap = 0
            for element in df:
                if article["url"] == element[2]:
                    overlap += 1       
            
            if overlap == 0:
                df_s = pd.read_csv(NEWS_DATA).append({"publishedAt": article["publishedAt"],
                                                        "title": article["title"],
                                                        "url": article["url"],
                                                        "isVisible": True,
                                                        "content": article["content"]},
                                                        ignore_index=True)

                df_s.to_csv(NEWS_DATA, index=False)

        elif len(df) == 0:
            df_s = pd.read_csv(NEWS_DATA).append({"publishedAt": article["publishedAt"],
                                                "title": article["title"],
                                                "url": article["url"],
                                                "isVisible": True,
                                                "content": article["content"]},
                                                ignore_index=True)

            df_s.to_csv(NEWS_DATA, index=False)
    return data

@func_logger
def update_news(update_interval, update_name, isRepeatable = False):
    """Creates an element in news que with given interval and name (can be Repeatable)"""
    # get todays date info
    today = date.today()
    # make an ISO datetype by joining date + provided interval - time, and parse it
    hole_date = parser.isoparse(str(today) + " " + update_interval)
    #print(hole_date)
    # make a time tuple for mktime function
    conv_date_tuple = hole_date.timetuple()
    # calculate needed interval from epoch, will be used for queing updates
    interval = time.mktime(conv_date_tuple)
    #print("interval: ", interval)
    # get now from epoch
    now = time.time()
    #print("Now: ", now)

    if interval <= now:
        # if interval less than now, we move point in future for 24 hr
        interval += 60*60*24
        # refactor to news_API_request function
        sched_obj = news_scheduler.enterabs(interval,1, news_API_request, ())

    elif interval > now:
        # refactor to news_API_request function
        sched_obj = news_scheduler.enterabs(interval,1, news_API_request, ())

    # Open filename at NEWS_UPDATE_QUE where the information about que is stored
    # and appned new line with data
    df = pd.read_csv(NEWS_UPDATE_QUE, index_col="index").append({"content": update_interval,
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
    df.to_csv(NEWS_UPDATE_QUE, index=True, index_label="index")

    return sched_obj

@func_logger
def news_art(return_df=0):
    """Makes and returns list of visible news to display in browseer"""
    df_n = pd.read_csv(NEWS_DATA)
    if len(df_n) > 0:
        df_n = df_n[df_n["isVisible"].astype(str) == "True"]
        df_n = df_n[["title","content","url"]]
        result = df_n.iloc[::-1] # reverse of DF
        result = result.to_dict(orient="records")
        if (return_df == 1):
            return df_n
        return result
    return []

@func_logger
def change_article_visibility(news_element):
    """Changes article visibility by given URL"""
    df_s = pd.read_csv(NEWS_DATA)

    if len(df_s)>0:

        elem_index = df_s.index[df_s["url"] == str(news_element)][0]

        if df_s.at[elem_index,"isVisible"] == False:
            df_s.at[elem_index,"isVisible"] = True
            print("Changed to:", df_s.at[elem_index,"isVisible"])
            df_s.to_csv(NEWS_DATA, index=False)
            return "Visibility changed"
        elif df_s.at[elem_index,"isVisible"] == True:
            df_s.at[elem_index,"isVisible"] = False
            print("Changed to:", df_s.at[elem_index,"isVisible"])
            df_s.to_csv(NEWS_DATA, index=False)
            return "Visibility changed"
    return "No elements in data file with news"

@func_logger
def check_outdated_news_que():
    """Check if there is outdated requests in news que, if so delete them from que"""
    check_outdated_data_que(file_name_open=NEWS_UPDATE_QUE, file_name_save=NEWS_UPDATE_QUE)
    

@func_logger
def restore_news_que(filename=NEWS_UPDATE_QUE):
    """Restores news que from file"""
    #restore_data_que(filename=NEWS_UPDATE_QUE)
    if news_scheduler.empty() is True:
        with open(filename,"r") as file:
            reader = csv.reader(file)
            next(reader)
            df_s = list(reader)
            if len(df_s)>0:
                for element in df_s:
                    news_scheduler.enterabs(float(element[3]),1, news_API_request, ())
                    return "Restored news que"
            return "Data que is empty"
    else:
        return "Current news que is not empty"
    
    
@func_logger
def delete_from_news_que(que_index=0,filename=NEWS_UPDATE_QUE):
    """Removes element from news que and news que file using index"""
    #delete_from_data_que(que_index, filename=NEWS_UPDATE_QUE)
    if news_scheduler.empty() is not True and len(news_scheduler.queue) > que_index:
        # delete element from que
        news_scheduler.cancel(news_scheduler.queue[que_index])

        # delete que_index+1 row in csv file
        df = pd.read_csv(filename, index_col="index", skiprows=[que_index+1])
        df.reset_index(inplace=True,drop=True)
        df.to_csv(filename, index=True, index_label="index")
        check_outdated_news_que()
        return f"News que element with index {que_index} deleted"
    else:
        return "Delete is not possible"
