# Homework 01: ask abot flights data (5 questions)
## question 1: How many different tail numbers in each carrier?
View(flights)

flights %>% 
  group_by(carrier) %>%
  distinct(tailnum) %>% 
  count(tailnum) %>% 
  summarise(engines_type = sum(n))

sqldf("select a.carrier,count(a.carrier) from
      (select carrier, tailnum from flights group by carrier, tailnum) as a
      group by carrier")

## question 2: count Top 5 destination in May 2013 .
flights %>% 
  filter(month == 5, year == 2013) %>% 
  count(dest) %>% 
  arrange(-n) %>% 
  head(5)

sqldf("select dest, count(dest) as n from flights where month = 5 and year = 2013
      group by dest order by n desc limit 5")

## question 3: Top 5 airlines that have the longest distance in Jan,2013
flights %>% 
  filter(month == 1, year == 2013) %>% 
  group_by(carrier) %>% 
  summarise(longest_distance = max(distance)) %>% 
  arrange(desc(longest_distance)) %>% 
  head(5)

sqldf("select carrier, max(distance) as longest_distance from flights 
      where month = 1 and year = 2013 group by carrier order by longest_distance
      desc limit 5")

## question 4: What is the most popular origin-destination pair in 2013
flights %>% 
  filter(year == 2013) %>% 
  mutate(pair = paste(origin,"-",dest)) %>% 
  count(pair) %>% 
  arrange(-n) %>% 
  head(1)

flights %>% 
  count(origin,dest) %>% 
  arrange(-n) %>% 
  head(1)


sqldf("select origin, dest, count(origin) from flights group by origin, dest
      order by count(origin) desc limit 1")

## question 5: Top 3 airlines with the most airtime for each month
flights %>% 
  group_by(month,carrier) %>% 
  summarise(total_air = sum(air_time,na.rm=T)) %>% 
  mutate(ranking = rank(-total_air)) %>% 
  filter(ranking < 4) %>% 
  arrange(month,ranking) %>% 
  mutate(month = month.abb[month]) %>% 
  left_join(airlines,by = "carrier") %>% 
  select(month,name,total_air,ranking) %>% View()

sqldf("select 
      a.month,
      a.carrier, 
      a.sum 
      from 
      (select 
        *,
        sum(air_time) as sum, 
        row_number() over(partition by month order by sum(air_time) desc) as row
      from flights where air_time is not null
      group by month,carrier) as a 
      where a.row < 4")

## question 6:  The most popular destination for each month
flights %>% 
  group_by(month,dest) %>% 
  summarise(n = n()) %>% 
  mutate(ranking = rank(-n)) %>% 
  filter(ranking == 1) %>% 
  left_join(airports, by = c('dest' = 'faa')) %>% 
  select( name, n)

sqldf("select
      airports.name ,
      a.sum 
      from (select 
        dest, 
        count(dest) as sum, 
        row_number() over(partition by month order by count(dest) desc) row 
       from flights group by month,dest) a 
      left join airports on dest = faa 
      where a.row = 1 ")

# Homework 02: create database on PostgreSQL
## => a few tables about pizza restaurants
library(DBI)
library(RPostgreSQL)
require("RPostgreSQL")

library(odbc)
conn <- dbConnect(PostgreSQL(),
                          user = "gypgfdlm",
                          password = "MSeBRq-rsxxE4CMAqNyJRt5wML7etJQ7",
                          host = "arjuna.db.elephantsql.com",
                          port = 5432,
                          dbname = "gypgfdlm")
dbListTables(conn)

pizza_menu <- data.frame(pizza_id = 1:6,
                         pizza_name = c('Hawaiian','Tomatoes','Cheese'),
                         pizza_price = c(120,130,130,120,115,110))

drink_menu <- data.frame(drink_id = 1:3,
                         drink_name = c('Pepsi','Beer','Water'),
                         drink_price = c(20,30,10))

#customers are allowed to order no more than 2 pizza flavors and 1 refreshing flavor
billing <- data.frame(bill_id = 1:25,
                         month = c(rep(1,12),rep(2,13)),
                         food_id1 = c(2,3,4,5,3,
                                      4,2,2,3,4,
                                      5,6,4,2,1,
                                      2,3,4,3,6,
                                      3,2,3,2,5),
                         num_id1 = c(1,1,2,1,3,
                                     4,1,2,3,4,
                                     2,3,2,1,5,
                                     2,3,1,1,2,
                                     3,4,5,2,1),
                         food_id2 = c(NA, NA, 6,6,NA,
                                      NA,3,4,NA,5,
                                      6,NA,NA,NA,2,
                                      NA,NA,5,NA,2,
                                      NA,NA,NA,4,NA),
                         num_id2 = c(NA,NA,1,2,NA,
                                     NA,1,1,NA,2,
                                     2,NA,NA,NA,1,
                                     NA,NA,1,NA,1,
                                     NA,NA,NA,2,NA),
                         drink_id3 = c(1,2,3,3,2,
                                      1,2,2,3,3,
                                      3,2,1,2,1,
                                      3,2,1,1,1,
                                      2,3,1,1,1),
                         num_id3 = c(2,2,4,5,6,
                                     3,4,5,7,7,
                                     3,4,5,2,1,
                                     2,3,4,1,2,
                                     3,4,5,6,2))

dbWriteTable(conn,"pizza_menu",pizza_menu)                  
dbWriteTable(conn,"drink_menu",drink_menu)  
dbWriteTable(conn,"billing",billing)  

dbListTables(conn)

## Let's query
pizza <- dbGetQuery(conn,"select * from pizza_menu")
drink <- dbGetQuery(conn,"select * from drink_menu")
bill <- dbGetQuery(conn,"select * from billing")

## Question 1: Create a query that shows the total sum of each bill
total_bill <- bill %>% 
  left_join(pizza, by = c('food_id1' = 'pizza_id')) %>%
  left_join(pizza, by = c('food_id2' = 'pizza_id')) %>% 
  left_join(drink, by = c('drink_id3' = 'drink_id')) %>% 
  mutate(pizza_price.y = replace_na(pizza_price.y,0),
         num_id2 = replace_na(num_id2,0),
         pizza_name.y = replace_na(pizza_name.y,'-')) %>% 
  mutate(total = pizza_price.x*num_id1 + pizza_price.y*num_id2
         + drink_price*num_id3) %>% 
  select(bill_id,month, pizza1 = pizza_name.x, numbers1 = num_id1,
         pizza2 = pizza_name.y, numbers2 = num_id2,
         drink = drink_name, numbers3 = num_id3, total)

View(total_bill)

## Question 2: use total_bill to find average bill in each month
total_bill %>% 
  group_by(month) %>% 
  summarise(average = mean(total))

## Question 3: list top 2 favorite pizzas in each month
total_bill %>% 
  group_by(month, pizza1) %>% 
  summarise(sum = sum(numbers1, numbers2)) %>% 
  mutate(rank = rank(-sum)) %>% 
  filter(rank < 3) %>% 
  select(month, fav_pizza = pizza1, sum)

## Question 4: list the most favorite drink in each month
total_bill %>% 
  group_by(month, drink) %>% 
  summarise(sum = sum(numbers3)) %>% 
  mutate(rank = rank(-sum)) %>% 
  filter(rank == 1) %>% 
  select(month, drink, sum)

## Question 5: list 2 top spenders in each month
total_bill %>% 
  group_by(month) %>% 
  summarise(top_spender = max(total)) 