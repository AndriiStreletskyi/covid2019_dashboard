#structure of COVID19 data / API fields
cases_and_deaths = {
    "areaCode": "areaCode",
    "areaName": "areaName",
    "areaType": "areaType",
    "date": "date",
    "cumDailyNsoDeathsByDeathDate": "cumDailyNsoDeathsByDeathDate",
    "hospitalCases": "hospitalCases",
    "newCasesBySpecimenDate": "newCasesBySpecimenDate"
}

QUE_UPDATE_FILE = "data_update_que.csv" # file name/path where data que is sotored
NEWS_UPDATE_QUE = "news_update_que.csv" # file name/path where news que is sotored
API_KEY = "6e309466481b4724a50b469c2370a670" # API key for news updates
NEWS_DATA = "news_data.csv" # file name/path that stores news data
COVID_DATA = "update_data.csv" # file name/pat that stores COVID19 data
LOCATION = "Exeter" # default location in API request for COVID19 data
LOCATION_TYPE = "ltla" # default location type in API request for COVID19 data
NATION_LOCATION = "England" # default nation to show on main screen
