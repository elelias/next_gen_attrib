#
# This is the server logic of a Shiny web application. You can run the 
# application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
# 
#    http://shiny.rstudio.com/
#

# Define server logic required to draw a histogram
function(input, output) {
   
  mktng_events_ds <- reactive({
    events_by_date <- mktng_events %>% 
    filter(event_type_id %in% input$MKTNG_EVENT) %>%
    select(-event_type_id) %>% 
    group_by(event_dt, channel_name) %>% 
    summarise(no_events = sum(no_events)) %>%
    spread(key = channel_name, value = no_events, fill = 0) %>%
    gather(channel_name, no_events, -event_dt, convert = TRUE)
    
    lapply(unique(events_by_date$channel_name),
                 function(x){
                   list(data = (events_by_date %>% 
                                  filter(channel_name == x) %>% 
                                  arrange(event_dt))$no_events,
                        name = x,
                        visibility = !(x %in% excluded))
                 })
  })
  
  mktng_events_strings <- reactive({
   
    res <- data.frame(title = "Proportion of marketing events per channel",
                 yaxis = "Percentage")
    
    if (input$MKTNG_TYPE_PLOT == "normal") {
      res <- data.frame(title = "Absolute number of marketing events per channel",
                 yaxis = "Number of events")
    }
    
    res
  })
  
  output$mktng_events_hc <- renderHighchart2({
    
    hc <- highchart() %>% 
      hc_chart(type = "area") %>% 
      hc_title(text = mktng_events_strings()$title) %>% 
      hc_subtitle(text = "Next Generation Attribution") %>% 
      hc_xAxis(categories = seq(from = as.Date("2017-05-01"),
                                to = as.Date("2017-07-01"),
                                by = 1),
               #(unique(events_by_date$event_dt)),
               tickmarkPlacement = "on",
               title = list(text = "Date Marketing Event")) %>% 
      hc_yAxis(title = list(text = mktng_events_strings()$yaxis)) %>% 
      hc_tooltip(headerFormat = 'Date: {point.key} <table>',
                 pointFormat = "<br/><span style=\"color:{series.color}\">{series.name}</span>:
                                    <b>{point.percentage:.1f}%</b> ({point.y:,.0f} events)",
                 crosshairs = TRUE, 
                 borderWidth = 3, 
                 #sort = TRUE, 
                 table = TRUE) %>% 
      hc_plotOptions(area = list(
        stacking = input$MKTNG_TYPE_PLOT,
        lineColor = "#ffffff",
        lineWidth = 1,
        marker = list(
          lineWidth = 1,
          lineColor = "#ffffff"
        ))
      ) %>%
      hc_add_series_list(mktng_events_ds()) %>%
      hc_exporting(
        enabled = TRUE
      )
    })
  
  relative_importance_proportions <- reactive({
    mktng_events_bbowa_daily_prop <- mktng_events_bbowa %>% 
      filter(bbowa_session_start_dt == as.Date(input$RELATIVE_IMPORTANCE_DATE)) %>%
      filter(event_type_id %in% input$RELATIVE_IMPORTANCE_EVENT) %>%
      select(-event_type_id) %>% 
      group_by(event_dt, channel_name) %>% 
      summarise(daily_events_channel = sum(no_events)) %>%
      ungroup(channel_name) %>%
      group_by(event_dt) %>%
      mutate(daily_events = sum(daily_events_channel)) %>%
      mutate(prop_evts_txns = daily_events_channel/daily_events*100) %>%
      arrange(event_dt, channel_name)
    
    left_join(mktng_events_bbowa_daily_prop %>%
                               select(event_dt,
                                      channel_name,
                                      prop_evts_txns),
                             mktng_events_prop %>% select(event_dt,
                                                          channel_name,
                                                          prop_evts),
                             by = c('event_dt', 'channel_name'))
    
    })
  
  output$relative_importance_hc <- renderHighchart2({
    hc <- hchart(relative_importance_proportions(),
                 type = "line",
                 hcaes(x = event_dt,
                       y = prop_evts_txns/prop_evts,
                       group = channel_name),
                 visible = !(c(sort(unique(relative_importance_proportions()$channel_name)), "NA") %in% excluded)) %>%
      hc_xAxis(title = list(text = "Date of the Marketing Event")) %>%
      hc_yAxis(title = list(text = "Ratio")) %>%
      hc_title(text = paste("Relative proportion of traffic tied to a BBOWAC event on ",
                            input$RELATIVE_IMPORTANCE_DATE,
                            "attributed to channel with respect to overall marketing-driven traffic")) %>%
      hc_tooltip(headerFormat = 'Date: {point.key}<table>',
                 pointFormat = "<br/><span style=\"color:{series.color}\">{series.name}</span>:
                                 <b>{point.y:,.2f}x</b>",
                  #shared = TRUE
                  crosshairs = TRUE, 
                  borderWidth = 3, 
                  sort = TRUE, 
                  table = TRUE) %>% 
      hc_exporting(
        enabled = TRUE
      )
  })
  
  output$correlated_txns_dt <- renderDataTable({
    datatable(
      averageClicks,
      rownames = FALSE,
      # caption = 'Table 1: This is a simple caption for the table.',
      extensions = 'Buttons',
      options = list(
        columnDefs = list(list(autoWidth = TRUE, className = 'dt-left', targets = c(0))),
        pageLength = 50,
        dom = 'Bfrtip',
        buttons = c('copy', 'csv')
      )) %>%
      formatRound(names(averageClicks)[-1], 2)
  })
  
  # cnts_r <- reactive({
  #   # cnts_temp <- cnts %>% group_by(channel_name) %>% mutate(prcnt=round(cnt/sum(cnt)*100, 1)) %>% ungroup()
  #   # cnts_temp$channel_name[is.na(cnts$channel_name)] <- "isNA"
  #   cnts_temp2 <- cnts_temp %>% group_by(channel_name) %>% arrange(day_diff) %>% mutate(cumm_cnt=cumsum(cnt)) %>% 
  #     select(channel_name, day_diff, cumm_cnt) %>% spread(channel_name, cumm_cnt)
  #   
  #   list(chart = cnts_temp,
  #        moving = cnts_temp2)
  #   })
  
  cnts_chart <- reactive({
    cnts_temp <- cnts %>% group_by(channel_name) %>% mutate(prcnt=round(cnt/sum(cnt)*100, 1)) %>% ungroup()
  cnts_temp$channel_name[is.na(cnts$channel_name)] <- "isNA"
  cnts_temp
  })
  
  cnts_moving <- reactive({
    cnts_temp <- cnts %>% group_by(channel_name) %>% mutate(prcnt=round(cnt/sum(cnt)*100, 1)) %>% ungroup()
    cnts_temp$channel_name[is.na(cnts$channel_name)] <- "isNA"
    cnts_temp2 <- cnts_temp %>% group_by(channel_name) %>% arrange(day_diff) %>% mutate(cumm_cnt=cumsum(cnt)) %>% 
      select(channel_name, day_diff, cumm_cnt) %>% spread(channel_name, cumm_cnt)
    
    cnts_temp2
  })
  
  output$lookback_period_hc <- renderHighchart2({
    
    hc <- hchart(cnts_chart() %>% arrange(channel_name, day_diff) %>% group_by(channel_name) %>% 
             mutate(cumm_cnt=cumsum(cnt), cumm_prcnt=cumsum(prcnt)),
           "line", 
           hcaes(x = day_diff, 
                 y = cumm_prcnt, 
                 group = channel_name),
           visible = !(unique((cnts_chart() %>% arrange(channel_name))$channel_name) %in% excluded)) %>% 
      hc_xAxis(title = list(text = "Days from Marketing event to first BBOWAC event")) %>% 
      hc_yAxis(title = list(text = "Percentage"), max = 100) %>% 
      hc_title(text = "Cummulative Percentage of Correlated Transactions ") %>%
      hc_tooltip(headerFormat = '{point.key} days <table>',
                 pointFormat = "<br/><span style=\"color:{series.color}\">{series.name}</span>:
                                  <b>{point.y:,.0f}%</b>",
                 crosshairs = TRUE,
                 borderWidth = 2,
                 table = TRUE,
                 sort = TRUE) %>% 
      hc_exporting(enabled = TRUE)
  })
  
  downloadHandler({
    cnts3 <- cnts_chart() %>% group_by(channel_name) %>% arrange(day_diff) %>% mutate(cumm_cnt=cumsum(cnt)) %>% 
      select(channel_name, day_diff, cumm_cnt) %>% spread(channel_name, cumm_cnt)
    hc <- highchart() %>% hc_chart(type = "column") %>% hc_xAxis(categories = " ")
    
    
    # ds <- lapply(names(cnts3)[-1], function(x){list(data = list(list(sequence = setNames(unlist(cnts3[x]), NULL))),
    #                                                 name = x,
    #                                                 visibility = !(x %in% excluded))})
    # hc <- hc %>% hc_add_series_list(ds)
    
     for(x in names(cnts3)[-1]){
       hc <- hc %>% hc_add_series(name = x, visible = ifelse(x %in% excluded, FALSE, TRUE),
                                  data = list(list(sequence = setNames(unlist(cnts3[x]), NULL))))
     }
    
    hc <- hc %>% 
      hc_motion(enabled = TRUE, labels = 0:61, series = 1:(length(names(cnts3[-1]))-1)) %>%
            hc_title(text = "Number of Correlated Transactions by Lookback Window")
    
    htmlwidgets::saveWidget(widget = hc, file = paste(getwd(), "/www/looback_period_hc_moving.html", sep = ""), selfcontained = FALSE)
      
  })
  
  output$lookback_period_hc_moving <- renderUI({
    tags$iframe(src="looback_period_hc_moving.html", height=600, width="100%")
  })
    
  
  output$average_clicks_by_channel_FM_dt <- renderDataTable({  
  datatable(
    averageClicksBySegment,
    rownames = FALSE,
    # caption = 'Table 1: This is a simple caption for the table.',
    extensions = 'Buttons',
    options = list(
      columnDefs = list(list(autoWidth = TRUE, className = 'dt-left', targets = c(0))),
      pageLength = 50,
      dom = 'Bfrtip',
      buttons = c('copy', 'csv')
    ))  %>%
    formatRound(names(averageClicksBySegment)[-1], 1)
  })
  
  output$transactions_cummulative_dt <- renderDataTable({
    txns_cummulative <- read_csv("data/transactions.csv")
      
    datatable(
      txns_cummulative,
      rownames = FALSE,
      # caption = 'Table 1: This is a simple caption for the table.',
      extensions = 'Buttons',
      options = list(
        columnDefs = list(list(autoWidth = TRUE, className = 'dt-left', targets = c(0))),
        pageLength = 50,
        dom = 'Bfrtip',
        buttons = c('copy', 'csv')
      ))  %>%
      formatRound(names(txns_cummulative)[2], 0)%>%
      formatRound(names(txns_cummulative)[3], 2)
  })
  
  # txns_cummulative_data_countries <- reactive({
  #   channels <- tr[[1]]$channel_name
  #   
  #   # devices <- if (input$DEVICE_TYPE_LEVEL2 == "All"){unique(transactions_by_countries$bbowa_device_type_level2)} else input$DEVICE_TYPE_LEVEL2
  #   
  #   M_normalisation <- (transactions_by_countries %>% 
#                         filter(bbowa_site_name %in% input$NTRIES) %>% 
  #                         filter(daydiff == input$BASELINE_LOOKBACK) %>%
  #                         filter(channel_name %in% channels) %>%
  #                         select(-bbowa_site_id) %>% 
  #                         group_by(daydiff, channel_name) %>% 
  #                         summarise(no_txns = sum(transcactions)))$no_txns
  #   
  #   M <- transactions_by_countries %>% 
  #     filter(bbowa_site_name %in% input$TXNS_COUNTRIES) %>% 
  #     filter(channel_name %in% channels) %>%
  #     filter(daydiff <= input$WINDOW_RANGE[2]) %>%
  #     filter(daydiff >= input$WINDOW_RANGE[1]) %>%
  #     select(-bbowa_site_id) %>% 
  #     group_by(daydiff, channel_name) %>% 
  #     summarise(no_txns = sum(transcactions)) %>%
  #     spread(key = daydiff, value = no_txns)
  #   N <- as.matrix(M[-1])
  #   cbind(tr[[1]][1], diag(1/M_normalisation) %*% N * 100)
  #   # which_cols <- (as.numeric(colnames(M)) <= as.numeric(input$WINDOW_RANGE[2])) & (as.numeric(colnames(M)) >= as.numeric(input$WINDOW_RANGE[1]))
  #   # View(M[,c(which_cols)] * (1/as.vector(M[, input$BASELINE_LOOKBACK])) * 100)
  #   
  # })
  
  txns_cummulative_data_countries_device <- reactive({
    channels <- tr[[1]]$channel_name
    channels <- c(
      "Display",
      "epn",
      "marketing_emails",
      "Natural Search",
      "Paid Search",
      "Paid Social",
      "site_emails",
      "Social Media"
    )
    
    countries <- if (input$TXNS_COUNTRIES == "All"){unique(transactions_by_countries_device$bbowa_site_name)} else input$TXNS_COUNTRIES
    devices <- if (input$DEVICE_TYPE_LEVEL2 == "All"){unique(transactions_by_countries_device$bbowa_device_type_level2)} else input$DEVICE_TYPE_LEVEL2
    
    
    
    M_normalisation <- transactions_by_countries_device %>% 
                          filter(bbowa_site_name %in% countries) %>% 
      filter(bbowa_device_type_level2 %in% devices) %>%
      filter(daydiff == input$BASELINE_LOOKBACK) %>%
      filter(channel_name %in% channels)
      
    present_channels <- unique(M_normalisation$channel_name)  
    
     M_normalisation <- (M_normalisation %>%
      select(-bbowa_site_id) %>% 
      group_by(daydiff, channel_name) %>% 
      summarise(no_txns = sum(transcactions)))$no_txns
    
    M <- transactions_by_countries_device %>% 
                filter(bbowa_site_name %in% countries ) %>% 
                filter(bbowa_device_type_level2 %in% devices) %>%
                filter(channel_name %in% present_channels) %>%
                filter(daydiff <= input$WINDOW_RANGE[2]) %>%
                filter(daydiff >= input$WINDOW_RANGE[1]) %>%
                select(-bbowa_site_id) %>% 
                group_by(daydiff, channel_name) %>% 
                summarise(no_txns = sum(transcactions))
    
    if (nrow(M) > 0) M %<>%
                spread(key = daydiff, value = no_txns)
    
    N <- as.matrix(M[-1])
    cbind(M[1], 
          diag(1/M_normalisation) %*% N * 100)
    # which_cols <- (as.numeric(colnames(M)) <= as.numeric(input$WINDOW_RANGE[2])) & (as.numeric(colnames(M)) >= as.numeric(input$WINDOW_RANGE[1]))
    # View(M[,c(which_cols)] * (1/as.vector(M[, input$BASELINE_LOOKBACK])) * 100)
    
  })
  
  
  transactions_cummulative_data <- reactive({
    M <- as.matrix(tr[[1]][-1])
    which_cols <- (as.numeric(colnames(M)) <= as.numeric(input$WINDOW_RANGE[2])) & (as.numeric(colnames(M)) >= as.numeric(input$WINDOW_RANGE[1]))
    cbind(tr[[1]][1],
         M[,which_cols] * (1/as.vector(M[, input$BASELINE_LOOKBACK])) * 100)
  })
  
  output$transactions_cummulative_plot <- renderHighchart2({
    hchart(txns_cummulative_data_countries_device() %>% gather(day_diff, prcnt_dist, -channel_name) %>% filter(channel_name != "") %>% 
             mutate(day_diff=as.numeric(day_diff), 
                    prcnt_dist=as.numeric(sub("%", "", prcnt_dist))) %>% 
             arrange(channel_name, day_diff)
           ,"line", hcaes(x = day_diff, y = prcnt_dist, group = channel_name)) %>% 
      hc_xAxis(title = list(text = "Days from Marketing event to first BBOWAC event")) %>% 
      hc_yAxis(title = list(text = "% Correlated Transactions")) %>% 
      hc_title(text = "% of correlated transactions by channel by days diff") %>% 
      hc_tooltip(headerFormat = '{point.key} days <table>',
                 pointFormat = "<br/><span style=\"color:{series.color}\">{series.name}</span>:
                                  <b>{point.y:,.1f}%</b>",
                 crosshairs = TRUE,
                 borderWidth = 2,
                 table = TRUE) %>% 
      hc_exporting(enabled = TRUE)
    
  })
  
  output$transactions_channel_dates_dt <- renderDataTable({  
    datatable(
      tr[[1]],
      rownames = FALSE,
      # caption = 'Table 1: This is a simple caption for the table.',
      extensions = 'Buttons',
      options = list(
        columnDefs = list(list(autoWidth = TRUE, className = 'dt-left', targets = c(0))),
        pageLength = 50,
        dom = 'Bfrtip',
        buttons = c('copy', 'csv')
      ))  %>%
      formatRound(names(tr[[1]])[-1], 0)
  })
  
  
  downloadHandler({
    multichannel_site <- c(0, 3, 77)

    f <- function(x){
      switch(as.character(x),
             "0" = "US",
             "3" = "UK",
             "77" = "DE")
    }
    countries <- paste(sapply(multichannel_site,
                              f),
                       sep = "' '",
                       collapse = ", ")
    
    
    hc <- highchart()

    for (x in unique(multichannel$site)){#intersect(unique(multichannel$site), multichannel_site)){
      rid <- random_id()
      
      ctry_multichannel <- multichannel %>% filter(site == x)
      ctry_multichannel$low <- ifelse(ctry_multichannel$mean - ctry_multichannel$stddev, 1, ctry_multichannel$mean - ctry_multichannel$stddev)
      ctry_multichannel$high <- ctry_multichannel$mean + ctry_multichannel$stddev
      
      hc <- hc %>% hc_add_series(multichannel %>% filter(site == x),
                                 id = rid,
                                 name = paste(f(x)),
                                 type = "line",
                                 hcaes(x = window,
                                       y = mean)
                                 # ,tooltip = list(
                                 #   enabled = TRUE
                                 #   #,lineColor = "#ffffff"
                                 # )
                                 ) %>%
        hc_add_series(multichannel %>% filter(site == x),
                      name = paste("Range ", f(x), sep = ""),
                      type = "arearange",
                      hcaes(x = window,
                            low = ifelse(mean - stddev < 1, 1, mean - stddev)
                            ,high = mean + stddev
                            ),
                      fillOpacity = 0.1,
                      linkedTo = rid
                      ,tooltip = list(
                         enabled = TRUE
                         ,lineColor = "#ffffff"
                       )
                      )
    }

    hc <- hc  %>%
      hc_xAxis(title = list(text = "Lookback window in days")) %>%
      hc_yAxis(title = list(text = "Average number of channels in click path")) %>%
      hc_title(text = paste("Number of different channels in click_path for ", countries, sep = "")) %>%
      hc_tooltip(enabled = TRUE,
                 crosshairs = TRUE,
                 headerFormat = '{point.key} days<br><table>') %>%
      #pointFormat = "<br/><span style=\"color:{series.color}\">{series.name}</span>:
      #     <b>{point.y:,.2f} ({point.low:,.2f}, {point.high:,.2f})</b>",
      #             #shared = TRUE
                  # crosshairs = TRUE,
                  # borderWidth = 3,
                  # table = TRUE,
                  # shared = TRUE) %>%
      hc_exporting(enabled = TRUE)
    
    htmlwidgets::saveWidget(widget = hc, file = paste(getwd(), "/www/multichannel.html", sep = ""), selfcontained = FALSE)
  })
  
  output$multichannel_html <- renderUI({
    my_test <- tags$iframe(src="multichannel.html", height=600, width="100%")
    print(my_test)
    my_test
  })
  
  cumul_multichannel <- read_tsv("data/cumul_multichannel.csv")
  cumul_multichannel$site <- as.factor(cumul_multichannel$site)
  cumul_multichannel$site <- plyr::revalue(cumul_multichannel$site, c("0" = "US", "3" = "UK", "77" = "DE"))
  
  output$multichannel_onechannel_hc <- renderHighchart({
    
    data <- cumul_multichannel %>% filter(n_channels == input$NUMBER_OF_CHANNELS) %>% select(cumul_frac, site, window)
    
    hchart(data,
           "line",
           hcaes(x = window,
                 y = cumul_frac * 100,
                 group = site)) %>%
      hc_xAxis(title = list(text = "Lookback window in days")) %>%
      hc_yAxis(title = list(text = "Percentage")) %>%
      hc_title(text = paste("Proportion of user paths with ", input$NUMBER_OF_CHANNELS, " channel", ifelse(input$NUMBER_OF_CHANNELS == 1, "", "s"), sep = "")) %>%
      hc_tooltip(headerFormat = '{point.key} days <table>',
                 pointFormat = "<br/><span style=\"color:{series.color}\">{series.name}</span>:
                                  <b>{point.y:,.1f}%</b>",
                 crosshairs = TRUE,
                 borderWidth = 2,
                 table = TRUE) %>% 
      hc_exporting(enabled = TRUE)
  })
  
  load(paste(getwd(), "/data/display.RData", sep = ""), envir=.GlobalEnv)

  output$display_cummulative_hc <- renderHighchart(hc3 %>% hc_tooltip(headerFormat = '{point.key} days <table>',
                                                                      pointFormat = "<br/><span style=\"color:{series.color}\">{series.name}</span>:
                                                                                         <b>{point.y:,.1f}%</b>",
                                                                       crosshairs = TRUE,
                                                                       borderWidth = 2,
                                                                       table = TRUE) %>%
                                                          hc_yAxis(title = list(text = "Cummulative % Correlated Clicks"))

                                                   )
  
  downloadHandler({
    htmlwidgets::saveWidget(hc4, file = paste(getwd(), "/www/hc4.html", sep = ""), selfcontained = FALSE)
  })
  output$display_cummulative_hc_motion <- renderUI({tags$iframe(src="hc4.html", height=600, width="100%")})

  # display_table_data <- reactive({
  #   return(ifelse(input$DISPLAY_TABLE_TYPE == 'normal', big_rock, big_rock2))
  # })
    
  
  output$display_cummulative_dt_plot <- renderHighchart2({
    data <- data.frame(cbind(big_rock[,1], as.matrix(big_rock)[,-1] %*% diag(1/as.matrix(big_rock)[(as.numeric(input$BASELINE_LOOKBACK_DISPLAY) + 1),-1]) * 100))
    names(data) <- names(big_rock)
    data %<>% gather(big_rocks, cumm_prcnt, -dayDiff)
    
    hchart(data,"line", hcaes(x = dayDiff, y = cumm_prcnt, group = big_rocks)) %>% 
      hc_xAxis(title = list(text = "Days from Marketing event to first BBOWAC event")) %>% 
      hc_yAxis(title = list(text = "Cummulative % Correlated Transactions")) %>% 
      hc_title(text = paste("Correlated transactions with ",
                            input$BASELINE_LOOKBACK_DISPLAY,
                            " day",
                            ifelse(input$BASELINE_LOOKBACK_DISPLAY == 1, "", "s"),
                            " baseline lookback window", sep = "")) %>%
      hc_tooltip(headerFormat = '{point.key} days <table>',
                 pointFormat = "<br/><span style=\"color:{series.color}\">{series.name}</span>:
                                  <b>{point.y:,.1f}%</b>",
                 crosshairs = TRUE,
                 borderWidth = 2,
                 table = TRUE) %>% 
      hc_exporting(enabled = TRUE)
  })
  
  output$display_cummulative_dt <- renderDataTable({
    display_table_data <- if(input$DISPLAY_TABLE_TYPE == 'normal') big_rock else big_rock2
    
    datatable(
      display_table_data,
    rownames = FALSE,
    # caption = 'Table 1: This is a simple caption for the table.',
    extensions = 'Buttons',
    options = list(
      columnDefs = list(list(autoWidth = TRUE, className = 'dt-left', targets = c(0))),
      pageLength = 65,
      dom = 'Bfrtip',
      buttons = c('copy', 'csv')
    ))  %>%
      formatRound(names(big_rock)[-1], ifelse(input$DISPLAY_TABLE_TYPE == 'normal', 0, 2))
    })
  
    output$simulation_scatter_hc <- renderHighchart({
      hc <- highchart() %>%
              hc_add_series_scatter(x = MF2,
                                    y = RT2,
                                    name = "Simulation") %>%
              hc_xAxis(title = list(text = "MF2")) %>%
              hc_yAxis(title = list(text = "RT2"), max = 100) %>%
              hc_exporting(enabled = TRUE)
    })
    
    
    
}
