-- Number of seats per plane by class

Select aircraft_code , fare_conditions , count(seat_no) as count From seats Group By aircraft_code , fare_conditions 
    order by aircraft_code ASC , count DESC;

-- Total number of canceled flights on different days of the week

SELECT day , count(day) From
    (SELECT TO_CHAR(date, 'Day') AS day From
        (SELECT TO_TIMESTAMP(scheduled_departure, 'YYYY-MM-DD')as date From flights where status = 'Cancelled'))
        Group By day order by count DESC, day ASC;

-- Ranking of the number of passengers transported at airports by different aircraft

Select airport ,
        model as aircraft_model ,
        count ,
        DENSE_RANK () Over (Partition By airport Order By Count DESC) as rank
    From aircrafts_data Join
    (Select airport_name::json->>'en' as airport , aircraft_code , count From airports_data Join
        (Select airport_code , aircraft_code , count(aircraft_code) as count From
            (Select  airport_code , aircraft_code, ticket_no From ticket_flights Join
                (Select flight_id , departure_airport as airport_code , aircraft_code From flights where status != 'Cancelled'
                    UNION ALL
                Select flight_id , arrival_airport as airport_code , aircraft_code From flights where status != 'Cancelled') as t2
            on ticket_flights.flight_id = t2.flight_id)
        Group By airport_code ,aircraft_code) as t3
    on airports_data.airport_code = t3.airport_code) as t4
    on aircrafts_data.aircraft_code = t4.aircraft_code 
    order by airport ,count DESC;


-- Time difference between different cities
Select * From airports_data;

Concat(LEAST(city_1 , city_2) , GREATEST(city_1, city_2))

Select SPLIT_PART(Concat(LEAST(city_1 , city::json->>'en') , ' ',GREATEST(city_1, city::json->>'en')), ' ',1) as city1,
        SPLIT_PART(Concat(LEAST(city_1 , city::json->>'en') , ' ',GREATEST(city_1, city::json->>'en')), ' ',2) as city2,
        AVG(time_diffrrence) as time_dist From airports_data Join
    (Select city::json->>'en' as city_1 , arrival_airport, time_diffrrence From airports_data Join
        (Select AVG(Extract(EPOCH From (TO_TIMESTAMP(scheduled_arrival, 'YYYY-MM-DD HH24:MI:SS') - TO_TIMESTAMP(scheduled_departure, 'YYYY-MM-DD HH24:MI:SS'))) / 60) as time_diffrrence,
            departure_airport, arrival_airport From flights
        Group By departure_airport, arrival_airport ) as t2
    On airports_data.airport_code = t2.departure_airport) as t3
On airports_data.airport_code = t3.arrival_airport
Group By Concat(LEAST(city_1 , city::json->>'en') , ' ',GREATEST(city_1, city::json->>'en'))
Order by time_dist DESC;
