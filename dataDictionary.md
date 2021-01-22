## Fact table: immigrations

1. id: int, unique identifier of the fact table
2. year: int, the year in which the immigrant entered the US
3. month: int, the month in which the immigrant entered the US
4. country_code: int, the country code of the immigrant
5. airport_id: string, the identifier of the airport where the immigrant landed
6. state_code: string, the US state_code where the immigrant entered
7. count: double, the count of the immigrant
8. city_code: string, the US city code where the immigrant entered
9. arrival_date: string, the date when the immigrant entered the US
10. immigrant_id: int, identfier of immigrant

## Dim table 1: immigrants

1. immigrant_id: int, unique identifier of immigrant
2. gender: string, the gender
3. visa_type: string, the visa type of immigrant e.g. B2
4. occupation: string, the intended occupation in the US of immigrant
5. mode: string, the model of travel of immigrant e.g. air
6. visa_category: string, the visa category of immigrant e.g work
7. country: string, the country of immigrant

## Dim table 2: states

1. state_code: string, unique code identifying the state
2. state: string, the name of the state
3. population: int, the population of the state
4. female_population: int, the number of females in the state
5. num_veterans: int, the number of veterans in the state
6. foreign_born: int, the number of foreign born population in the state
7. avg_households: float, the average household size of the state
8. native: float, the percentage of American Natives in the state
9. asian: float, the percentage of Asians in the state
10. black: float, the percentage of Blacks in the state
11. hispanic: float, the percentage of Hispanics in the state 
12. white: float, the percentage of Whites in the state

## Dim table 3: cities
1. city_code: string, unique code identifying the city
2. city: string,  Name of the city
3. state_code: string, state code
4. state: string, name of the State to which the city belongs
5. total_population: int, the total population of the city
6. female_population: int, the female population of the city
7. num_veterans: int, the number of veterans in the city
8. foreign_born: int, the number of foreign born in the city
9. avg_households: float, the average number of persons per household in the city
10. native: float, the percentage of Natives in the city
11. asian: float, the percentage of Asians in the city
12. black: float, the percentage of Blacks in the city
13. hispanic: float, the percentage of Hispanics in the city 
14. white: float, the percentage of Whites in the city

## Dim table 4: US Temperature
1. city_code: string, the city code 
2. date: string, the date of temperature
3. year: int, the year corresponding to the date
4. month: int, the month corresponding to the date
5. avg_temp: float, the average temperature in a given city on the given date
6. avg_temp_uncertainty: float, the average temperature uncertainty in a given city on the given date
7. latitude: float, the latitude value of the city
8. longitude: float the longitude value of the city

## Dim table 5: Airports
1. airport_id: string, unique identifier of the airport
2. city_code: string, the city code of state in which the airport is located
3. city: string, the name of the city in which the airport is located
4. state_code: string, the state code of state in which the airport is located
5. type: string, the type of airport one of (small, medium, large)
6. name: string, the name of the airport
7. elevation_ft: string, the elevation in feet of the airport
8. gps_code: string, the GPS coed of the airport
9. iata_code: string, the IATA code of the airoport
10. local_code: string, the local code of the airport
11. latitude: double, the latitude of the city of the airport
12. longitude: double the longitude of the city of the airport

## Dim table 6: Time
1. arrival_date: string, the date
2. year: int, the year
3. month: int, the month 
4. day: int, the day of the year
5. week: int, the week of the year
6. weekday: int the day of the week 1 for Sunday 7 for Saturday
