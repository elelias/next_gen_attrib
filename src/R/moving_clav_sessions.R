setwd("~/projects/next_gen_attribution/src/R")
suppressMessages(source("~/R_connector/libraries.R"))
suppressMessages(require(pystr))
hopper <- connToEDW(dataBase = "Hopper", connType = 'normal', user = 'pgurican', pass=trimws(read_file("~/R_connector/p.txt")))

sqlText <- paste("sel a.* from syslib.parallel_export(
  on (
    select 
    guid,
    cguid,
    parent_uid,
    
    cast(session_skey as varchar(30))||guid||cast(cast(site_id as int) as varchar(9))||cast(cobrand as varchar(8)) as event_id,
    5 as event_id_type,
    8 as event_type_id,
    
    0 as channel_id,
    \'organic\' as channel_name,
    
    null as rotation_id,
    null as rotation_name,
    
    null as campaign_id,
    null as campaign_name,
    
    session_start_dt as event_dt,
    start_timestamp as event_ts,
    
    null as incdata_id,
    null as clusted_id,
    
    device_type as device_id,
    experience_level1 as experience_level1,
    experience_level2 as experience_level2,
    device_type_level1 as device_level1,
    device_type_level2 as device_level2,
    
    cast(LNDG_PAGE_ID as varchar(16)) as flex_column1,
    null as flex_column2,
    null as flex_column3,
    null as flex_column4,
    gr_cnt as flex_column5,
    vi_cnt as flex_column6,
    
    -- partition
    
    \'organic\' as data_source,
    session_start_dt as dt
    
    from p_soj_cl_v.clav_session_ext 
    where SESSION_TRAFFIC_SOURCE_ID in (1,2,3) 
    and session_start_dt = \'{date}\'
  )
  using
  configname(\'hop_dm_apollo\')
  configserver(\'bridge-gateway-hop:1025\')
  sinkClass(\'HDFSTextFileSink\')
  nullValue(\'\\N\')
  delimiterCharCode(9)
  dataPath(\'/user/hive/warehouse/mktng.db/clav_sessions/dt={date}/clav\')
  fileErrorLimit(0)
  recv_eofresponse(1)
  sock_timeout(120)
) a;")

start_date <- as.Date("2017-06-28")
end_date <- as.Date("2017-10-01")


for (date in seq(start_date, end_date, by = 1))
{
  string_date <- as.Date(date)
  print(paste("Starting day: ", string_date, sep = ""))
  tryCatch({dbSendQuery(hopper,
                        pystr_format(sqlText,
                                     date = string_date))},
           warning = function(w) {print("warning")},
           error = function(e) {print("error")},
           finally = {})
}

missing_dates <- c("2017-05-15", 
           "2017-05-25", 
           "2017-05-29", 
           "2017-06-01", 
           "2017-06-05", 
           "2017-06-06", 
           "2017-06-10", 
           "2017-06-17", 
           "2017-06-22",
           "2017-06-23", 
           "2017-06-27",
           "2017-06-29",
           "2017-07-03",
           "2017-07-04",
           "2017-07-07",
           "2017-07-15",
           "2017-07-16",
           "2017-07-17",
           "2017-07-18",
           "2017-07-23",
           "2017-07-28",
           "2017-07-31",
           "2017-08-03",
           "2017-08-12",
           "2017-08-15",
           "2017-08-22",
           "2017-08-23",
           "2017-08-29",
           "2017-09-03",
           "2017-09-16",
           "2017-09-27")

failed_dates <- c()

for (string_date in missing_dates)
{
  print(paste("Starting day: ", string_date, sep = ""))
  tryCatch({dbSendQuery(hopper,
                        pystr_format(sqlText,
                                     date = string_date))},
           warning = function(w) {failed_dates <- c(failed_dates, string_date)},
           error = function(e) {failed_dates <- c(failed_dates, string_date)},
           finally = {})
}