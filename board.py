import re
import json
from flask import Flask, request, render_template
from global_params import LOCATION, NATION_LOCATION
from covid_news_handling import news_scheduler, news_art, check_outdated_news_que, restore_news_que, news_API_request, change_article_visibility, delete_from_news_que, update_news
from covid_data_handler import scheduler, check_outdated_data_que, restore_data_que, delete_from_data_que, schedule_covid_updates, joint_que, covid_API_request, process_covid_csv_data
import logger

with open("config.json") as f:
    config = json.load(f)

do_once = 0

app = Flask(__name__)
app.config.update(config)

@app.route("/", methods=["GET","POST"])
@app.route("/index", methods=["GET","POST"])
@app.route("/index.html", methods=["GET","POST"])
def board_index():

    global do_once

    if request.method == "GET":

        if do_once == 0:
            news_API_request()
            covid_API_request()
            do_once += 1

        check_outdated_news_que()
        restore_news_que()
        print("News que",news_scheduler.queue)

        check_outdated_data_que()
        restore_data_que()
        print("Data que",scheduler.queue)

        form_time = request.args.get("update")
        form_title = request.args.get("two")
        form_rep = request.args.get("repeat")
        form_data = request.args.get("covid-data")
        form_news = request.args.get("news")

        if form_time is not None and form_title is not None and (form_news is not None or form_data is not None) and (form_rep is None or form_rep is not None):
            if form_rep is None and (form_news is not None or form_data is not None):
                if(form_news):
                    update_news(str(form_time),str(form_title))
                if(form_data):
                    schedule_covid_updates(str(form_time),str(form_title))
            if form_rep is not None and (form_news is not None or form_data is not None):
                if(form_news):
                    update_news(str(form_time), str(form_title), True)
                if(form_data):
                    schedule_covid_updates(str(form_time), str(form_title), True)

        #print("Form data: ", form_time, form_title, form_rep, form_data, form_news)

    if request.method == "POST":

        element = request.form.get("update_item")
        news_element = request.form.get("notif")
        print(news_element)

        if news_element is not None:
            
            change_article_visibility(news_element)

        if element is not None:

            element_row = joint_que(1)[joint_que(1)["type"].astype(str) == element]
            element_type = re.search(r"[a-z]*", element,re.IGNORECASE).group()
            element_index_que = int(element_row["que_index"].values[0])
            if element_type == "data":
                delete_from_data_que(element_index_que)
            if element_type == "news":
                delete_from_news_que(element_index_que)

    if len(scheduler.queue) > 0:
        scheduler.run(blocking=False)
    if len(news_scheduler.queue) > 0:
        news_scheduler.run(blocking=False)
    return render_template('index.html',title="Covid19 Dashboard",updates=joint_que(),news_articles = news_art(),
                            location=LOCATION,
                            image = "covid19.jpg",
                            favicon = "static/images/favicon.ico",
                            local_7day_infections=process_covid_csv_data()[0],
                            nation_location=NATION_LOCATION,
                            national_7day_infections=process_covid_csv_data("nation_2021-10-28.csv")[0],
                            hospital_cases=process_covid_csv_data("nation_2021-10-28.csv")[1],
                            deaths_total=process_covid_csv_data("nation_2021-10-28.csv")[2])


if __name__ == "__main__":
    app.run(debug=True)
