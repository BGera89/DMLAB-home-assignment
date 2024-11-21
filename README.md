# Project Overview

This project shows historical and forecast weather data (about rain, humidity, tempreture) and air quality indexes (ozone, CO2 etc.) for a specific city. The user can choose from different Hungarian cities and the app will be able to display the weather/air quality data about these cities. 

# Project strucutre

The project is based a microservice architecture and currently there are three main microservices:
- The backbone of the operation is a PostgreSQL database where the data is being stored. This component also has an API where other microservices can interact with it. 
- The data gets fetcher by an API-fetcher from (insert link here).
- The data is being displayed by the UI component. The UI component is capable of making an API call to the fetcher component where it can get the data from the weather API and add it to the database.

## Microservice structure
### PostreSQL
The data comes in a tabular format and it's stored inside a PostgreSQL database. There are three main tables for the 3 parts of the API call. There is also one supplementary table where the Hungarian cities and their corresponding longitude/lattitude coordinates are stored. There is an idea to use a geotool to be able to fetch every city using: 
### DB init
When the user first boots up the app with docker compose the database has to initialize. This service makes sure to create the neccessary tables and append data to the supplementery one. 
### API fetcher
This service is responsible for fetching the weather information (historical, forecasts, and air quality data) from the insert link here.
This service also has an api_fetcher_api so it can communicate with other services (mostly the UI). 
### UI
The UI is a simple python-dash UI that displayes the fetched data from the API. It's index.py is written in a format so it can include multiple pages later on to be scalable. 
# Project pipeline

