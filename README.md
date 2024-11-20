# Project Overview

This project shows historical and forecast weather data (about rain, humidity, tempreture) and air quality indexes (ozone, CO2 etc.) for a specific city. The user can choose from different Hungarian cities and the app will be able to display the weather/air quality data about these cities. 

# Project strucutre

The project is based a microservice architecture and currently there are three main microservices:
- The backbone of the operation is a PostgreSQL database where the data is being stored. This component also has an API where other microservices can interact with it. 
- The data gets fetcher by an API-fetcher from (insert link here).
- The data is being displayed by the UI component. The UI component is capable of making an API call to the fetcher component where it can get the data from the weather API and add it to the database.

