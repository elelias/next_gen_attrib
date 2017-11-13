setwd("/srv/shiny-server/next_gen_attribution")

source("R_connector/connectEDW.R")
library(magrittr)
library(data.table)
library(RJDBC)
library(RSQLite)
library(DBI)
library(readr)
library(ggplot2)
library(dplyr)
library(tidyr)

formatNumber <- function(x) {formatC(x, format="d", big.mark=",")}

library(shiny)
library(highcharter)
library(shinydashboard)
library(DT)
library(lubridate)
library(stringr)
#library(pystr)
#library(googlesheets)
library(rJava)
library(zoo)
library(xts)
library(plotly)
library(reshape2)

########################### LOADING FUNCTIONS FOR DATA #####################################
hopper <- connToEDW(dataBase = "Hopper", connType = 'normal', user = 'pgurican', pass=trimws(read_file("R_connector/p.txt")))

###################### Excluded channels
cnts <- dbGetQuery(conn = hopper, "Select channel_name, day_diff, sum(cnt) as cnt from P_TRAFFIC_T.DP_TESTING_MOVER where event_type_id in (1, 5, 7) group by 1, 2")

cnts_temp <- cnts %>% group_by(channel_name) %>% mutate(prcnt=round(cnt/sum(cnt)*100, 1)) %>% ungroup()
cnts_temp$channel_name[is.na(cnts$channel_name)] <- "isNA"
cnts_temp2 <- cnts_temp %>% group_by(channel_name) %>% arrange(day_diff) %>% mutate(cumm_cnt=cumsum(cnt)) %>% 
                 select(channel_name, day_diff, cumm_cnt) %>% spread(channel_name, cumm_cnt)

excluded <- cnts %>% group_by(channel_name) %>% arrange(day_diff) %>% mutate(cumm_cnt=cumsum(cnt)) %>% 
  select(channel_name, day_diff, cumm_cnt) %>% filter(day_diff==61) %>% arrange(cumm_cnt) %>% select(-day_diff) %>% 
  filter(cumm_cnt < 1000000) %>%  select(channel_name) %>% unlist()

excluded <- c(excluded, "NULL", "isNA", "Natural Search - Gbase", "Others", "Non-IM Mktg Initiatives", 
              "Exclusion", "Paid Seach . Brand", "Affiliate", "Unassigned", "Shopping Comparison",
              "Shopping Comparison  - SDC", "Shopping Comparison - SDC", "Programmatic", "Series 20")

########################### Marketing Events
mktng_events <- dbGetQuery(conn = hopper, "select * from P_attribution_T.pg_agg_mktng_evts_v5;")
mktng_events <- mktng_events %>% select(-dt)
mktng_events$event_dt <- as.Date(mktng_events$event_dt)
mktng_events$channel_id <- as.factor(mktng_events$channel_id)
mktng_events$channel_name <- as.factor(mktng_events$channel_name)
#mktng_events$event_type_id <- as.factor(mktng_events$event_type_id)
mktng_events$experience_level2 <- as.factor(mktng_events$experience_level2)
mktng_events$device_level2 <- as.factor(mktng_events$device_level2)

########################## Relative importance
mktng_events_bbowa <- dbGetQuery(conn = hopper, "select event_dt, 
                                 channel_name, 
                                 event_type_id, 
                                 bbowa_session_start_dt,
                                 no_events
                                 from P_attribution_T.mktng_txns_v4_bbowa;")
mktng_events_bbowa$event_dt <- as.Date(mktng_events_bbowa$event_dt)
mktng_events_bbowa$channel_name <- as.factor(mktng_events_bbowa$channel_name)
mktng_events_bbowa$bbowa_session_start_dt <- as.Date(mktng_events_bbowa$bbowa_session_start_dt)

mktng_events_prop <- mktng_events %>% 
  select(-event_type_id, channel_id) %>% 
  group_by(event_dt, channel_name) %>% 
  summarise(daily_events_channel = sum(no_events)) %>%
  ungroup(channel_name) %>%
  group_by(event_dt) %>%
  mutate(daily_events = sum(daily_events_channel)) %>%
  mutate(prop_evts = daily_events_channel/daily_events*100) %>%
  arrange(event_dt, channel_name)

############################ Average clicks
averageClicks <- read_csv("data/averageClicks.csv")
names(averageClicks)[1] <- "channel_name"

averageClicksBySegment <- read_csv("data/bySegments.csv")


########################### Transactions
tr <- list(read.csv("data/tr1.csv", stringsAsFactors = FALSE),
           read.csv("data/tr2.csv", stringsAsFactors = FALSE),
           read.csv("data/tr3.csv", stringsAsFactors = FALSE))


names(tr[[1]]) <- c("channel_name", as.character(0:14), "30", "61")
names(tr[[2]]) <- c("channel_name", as.character(0:14), "30", "61")
names(tr[[3]]) <- c("channel_name", as.character(0:14), "30", "61")

tr[[1]] %<>% filter(!is.na(channel_name))
tr[[2]] %<>% filter(!is.na(channel_name))
tr[[3]] %<>% filter(!is.na(channel_name))
tr[[3]][, -1] <- apply(tr[[3]][, -1], 2, function(x) gsub("%", "", x) %>% as.numeric())

tr[[3]] %<>% gather(day_diff, prcnt_dist, -channel_name) %>% spread(channel_name, prcnt_dist) %>% 
  mutate(day_diff=as.numeric(day_diff)) %>% arrange(day_diff)

############################# Transactions by country
#transactions_by_countries_device <- read.csv(paste(getwd(), "/data/by_bbowac_country_id.csv", sep = ""), stringsAsFactors = FALSE)
#transactions_by_countries %<>% filter(keep == 1) %>% select(-keep)
#transactions_by_countries$bbowa_site_id <- as.factor(transactions_by_countries$bbowa_site_id)
#transactions_by_countries$bbowa_site_name <- as.factor(transactions_by_countries$bbowa_site_name)

############################ Transactions by country and device type id
transactions_by_countries_device <- read.csv(paste(getwd(), "/data/country_device.csv", sep = ""), stringsAsFactors = FALSE)
transactions_by_countries_device$bbowa_site_id <- as.factor(transactions_by_countries_device$bbowa_site_id)
transactions_by_countries_device$bbowa_site_name <- as.factor(transactions_by_countries_device$bbowa_site_name)
transactions_by_countries_device$bbowa_device_type_level2 <- as.factor(transactions_by_countries_device$bbowa_device_type_level2)

############################# Multichannel attribution
multichannel <- read_tsv("data/multichannel.csv")
cumul_multichannel <- read_tsv("data/cumul_multichannel.csv")
cumul_multichannel$site <- as.factor(cumul_multichannel$site)

############################ Display breakdown
load("data/display.RData", envir=.GlobalEnv)
# loaded "MF2"       "RT2"       "big_rock"  "hc4"       "hc3"       "big_rock2"