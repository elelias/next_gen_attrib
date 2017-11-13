#source("global.R")

dashboardPage(
  dashboardHeader(title = "Multi Touch Attribution"),
  
  dashboardSidebar(
    
    
    sidebarMenu(id = "tab",
                menuItem("Marketing Events", tabName = "marketing_events_tab", icon = icon("user-circle"))
                ,menuItem("Channels' Relative Importance", tabName = "relative_importance_tab", icon = icon("line-chart"))
                ,menuItem("Correlated Transactions", tabName = "correlated_txns_tab", icon = icon("shopping-basket"))
                ,menuItem("Lookback Period Overview", tabName = "lookback_period_tab", icon = icon("calendar-o"))
                ,menuItem("Clicks by FM Segment", tabName = "clicks_FM_tab", icon = icon("hand-pointer-o"))
                ,menuItem("Transactional Data", tabName = "transactions_tab", icon = icon("usd"))
                ,menuItem("Multichannel Analysis", tabName = "multichannel_tab", icon = icon("exchange"))
                ,menuItem("Display Breakdown", tabName = "display_tab", icon = icon("tv"))
    )
    # ,
    # br(),
    # br(),
    # br(),
    # br(),
    # uiOutput("out1")
  ),
  
  
  dashboardBody(
    tags$head(
      tags$script(src="https://code.highcharts.com/modules/boost.js"),
      tags$script(src="https://code.highcharts.com/highcharts-more.js"),
      tags$style(HTML("hr {border-top: 1px solid #000000;}"))
    ),
    
    tabItems(
      # First tab content
      tabItem(tabName = "marketing_events_tab",
              
              
              
              fluidRow(
                #highcharts goes here
                box(width = 6,
                    selectInput(inputId = "MKTNG_EVENT", 
                                label="Select Event Type", 
                                choices = c("Rover Clicks" = c(1),
                                            "Email Clicks" = c(5),
                                            "Simulated Clicks" = c(7),
                                            "Impressions" = c(4),
                                            "Opens" = c(6)),
                                selected = c(1, 5, 7),
                                multiple = TRUE)
                ),
                box(width = 3,
                    selectInput(inputId = "MKTNG_TYPE_PLOT",
                                label="Select Plot Type", 
                                choices = c("Percentage" = "percent",
                                            "Absolute numbers" = "normal"),
                                selected = "percent",
                                multiple = FALSE)
                )
              ),
              
              
              
                
                # Boxes need to be put in a row (or column)
                fluidRow(
                  box(highchartOutput2("mktng_events_hc",
                                      height = "550px"), width = 12)
                 )
      )
      
      ,



      # Second tab content
      tabItem(tabName = "relative_importance_tab",
              h2("Relative Importance of Channels with and without Correlations to BBOWA Events"),


              fluidRow(
                #highcharts goes here
                box(width = 3,
                    dateInput(inputId = "RELATIVE_IMPORTANCE_DATE", 
                              label = "Select date of BBOWAC event", 
                              value = "2017-07-01", 
                              min = "2017-06-22", 
                              max = "2017-07-01",
                              format = "yyyy-mm-dd", 
                              startview = "month", 
                              weekstart = 1,
                              language = "en", width = NULL)
                ),
                box(width = 6,
                    selectInput(inputId = "RELATIVE_IMPORTANCE_EVENT", 
                                label="Select Event Type", 
                                choices = c("Rover Clicks" = c(1),
                                            "Email Clicks" = c(5),
                                            "Simulated Clicks" = c(7),
                                            "Impressions" = c(4),
                                            "Opens" = c(6)),
                                selected = c(1, 5, 7),
                                multiple = TRUE)
                )
              ),
                # Boxes need to be put in a row (or column)
                fluidRow(
                  box(highchartOutput2("relative_importance_hc",
                                       height = "175%"), width = 12)
                )
              

      )
      ,

      # Third tab content
      tabItem(tabName = "correlated_txns_tab",
              h2("Correlated Transations"),
              h4(HTML("The average number of clicks for a correlated transaction is:<br/>
                      <ul><li><strong>3.31</strong> with a 1-day window</li>
                      <li><strong>4.05</strong> with a 7-day window</li>
                      <li><strong>6.26</strong> with a 1-month window</li></ul>"))

              # ,fluidRow(
              #   #highcharts goes here
              #   box(width = 3,
              #       selectInput("c_country", label="Select Country",
              #                   choices = c('UK','DE', 'IT'),
              #                   selected = "UK",
              #                   multiple = FALSE)
              #   )
              # 
              # 
              #   box(width = 3,
              #       selectInput("c_base", label="Select Base",
              #                   choices = c('All_asked'),
              #                   selected = "All_asked",
              #                   multiple = FALSE)
              #   ),
              # 
              # 
                 ,fluidPage(DT::dataTableOutput('correlated_txns_dt'))
               




      )
      , 
       
       # Fourth tab content
       tabItem(tabName = "lookback_period_tab",
               h3("From a marketing channel viewpoint"),
               
                 
                 # Boxes need to be put in a row (or column)
                 fluidRow(
                   box(highchartOutput2("lookback_period_hc",
                                        height = "100%"), width = 12)
                 )
               ,
               
               h3("Examining the number of correlated transactions by lookback window")
               

                 # Boxes need to be put in a row (or column)
                 ,fluidRow(
                   htmlOutput("lookback_period_hc_moving",
                                        height = "100%")
                 
                )
               
      #         wellPanel(
      #           helpText( a("Click Here to get data", target="_blank",
      #                       href="https://docs.google.com/a/ebay.com/spreadsheets/d/1zFWmYdfV5k7fxh-DC2gAwuk3UU2eGCgiG6IqJK87NMM/edit?usp=sharing"
      #           )
      #           )
      #         ),
      #         
      #         fluidRow(
      #           
      #           
      #           #image
      #           h3("UK offline Campaigns "),
      #           box(imageOutput("uk1"), width=12),
      #           h3("DE offline Campaigns "),
      #           box(imageOutput("uk2"), width=12)
      #         )
      )
      
      # Fifth tab content
      ,tabItem(tabName = "clicks_FM_tab",
              h2("Average Clicks by Channel and FM Segment")

              # ,fluidRow(
              #   #highcharts goes here
              #   box(width = 3,
              #       selectInput("c_country", label="Select Country",
              #                   choices = c('UK','DE', 'IT'),
              #                   selected = "UK",
              #                   multiple = FALSE)
              #   )
              # 
              # 
              #   box(width = 3,
              #       selectInput("c_base", label="Select Base",
              #                   choices = c('All_asked'),
              #                   selected = "All_asked",
              #                   multiple = FALSE)
              #   ),
              # 
              # 
              ,fluidPage(DT::dataTableOutput('average_clicks_by_channel_FM_dt'))
              
              
              
              
              
      )
      
      ,
      
      # Sixth tab content
      tabItem(tabName = "transactions_tab",
              h2("Transactions")
              
               ,fluidRow(
                 #highcharts goes here
                 box(width = 3,
                     selectInput("BASELINE_LOOKBACK", label="Select Baseline Lookback Window",
                                 choices = names(tr[[2]])[-1],
                                 selected = names(tr[[2]])[-1][2],
                                 multiple = FALSE)
                    )
                 
                 ,box(width = 3, 
                      sliderInput("WINDOW_RANGE", "Lookback window range:",
                                  min = as.numeric(names(tr[[2]])[-1][1]), 
                                  max = as.numeric(names(tr[[2]])[-1][length(names(tr[[2]])[-1])]),
                                  value = c(as.numeric(names(tr[[2]])[-1][1]),
                                            as.numeric(names(tr[[2]])[-1][length(names(tr[[2]])[-1])]))))
                 
                 
                  ,box(width = 3,
                       selectInput("TXNS_COUNTRIES", "Included countries:",
                                   choices = c("All", sort(levels(transactions_by_countries_device$bbowa_site_name))),
                                   selected = "All",
                                   multiple = FALSE))
                 
                 ,box(width = 3,
                      selectInput("DEVICE_TYPE_LEVEL2", "Devices :",
                                  choices = c("All", sort(levels(transactions_by_countries_device$bbowa_device_type_level2))),
                                  selected = "All",
                                  multiple = FALSE))
               )
              
      
              # 
              # 
              #   box(width = 3,
              #       selectInput("c_base", label="Select Base",
              #                   choices = c('All_asked'),
              #                   selected = "All_asked",
              #                   multiple = FALSE)
                 
              # 
              #
              
               #          )
               # 
               # ,fluidPage(
               #   tags$head(
               #     tags$link(rel = "stylesheet", type = "text/css", href = "custom.css")
               #   )
                 
                 # Boxes need to be put in a row (or column)
                 #,fluidRow(
              ,fluidRow(box(highchartOutput2("transactions_cummulative_plot"
                                              ,height = "650px"), width = 12))
              ,fluidRow(box(DT::dataTableOutput('transactions_cummulative_dt'), width = 12))
               ,fluidPage(DT::dataTableOutput('transactions_channel_dates_dt'), width = "100%")
              
              
              
              
              
      )
      
      ,
      
      
      
      # Seventh tab content
      tabItem(tabName = "multichannel_tab",
              class = "active",
              h2("Analysing the number of different channels in user's clickpath"),
              # fluidRow(
              #   box(width = 6,
              #       selectInput(inputId = "MULTICHANNEL_SITE", 
              #                   label="Select Site", 
              #                   choices = c("US" = c(0),
              #                               "UK" = c(3),
              #                               "DE" = c(77)),
              #                   selected = c(0, 3, 77),
              #                   multiple = TRUE)
              #   )
              # )
              # ,


                # Boxes need to be put in a row (or column)
              
              # fluidPage(column(12, tags$iframe(
              #   seamless="NA",
              #   scrolling="auto",
              #   height="100%",
              #   width="100%",
              #   frameborder="0",
              #   src="www/multichannel.html"),style='height: 100%'))
              
                fluidRow(
                  htmlOutput("multichannel_html")
                ),
              hr(),
              h3("Paths with a specific number of channels"),
              fluidRow(
              box(width = 3, 
                  sliderInput("NUMBER_OF_CHANNELS", "Number of channels:",
                              min = 1, 
                              max = 7,
                              value = 1))),
              
              fluidRow(
                box(highchartOutput("multichannel_onechannel_hc"),
                    width = 12)
              )
              
      )
      
      #Eighth tab contents
      ,tabItem(tabName = "display_tab",
               #fluidPage(DT::dataTableOutput('average_clicks_by_channel_FM_dt')),
               
               h3("Correlated Transactions according to strategy type"),
               
               
               # Boxes need to be put in a row (or column)
               fluidRow(
                 box(highchartOutput2("display_cummulative_hc",
                                      height = "100%"), width = 12)
               )
               ,
               hr(),
               
               h3("Cummulative view with absolute numbers")
               
               
               # Boxes need to be put in a row (or column)
               ,fluidRow(
                 htmlOutput("display_cummulative_hc_motion"),
                 width = 12,
                 height = 800
               )
               
               ,hr()
               
               ,fluidRow(
                 #highcharts goes here
                 box(width = 3,
                     selectInput("BASELINE_LOOKBACK_DISPLAY", label="Select Baseline Lookback Window",
                                 choices = 0:61,
                                 selected = 1,
                                 multiple = FALSE))
               )
              
               
               ,fluidRow(
                 box(highchartOutput2("display_cummulative_dt_plot"),
                     width = 12)
               )
               
               ,hr()
               
               ,fluidRow(
                 box(width = 3,
                     selectInput(inputId = "DISPLAY_TABLE_TYPE",
                                 label="Select Table Type", 
                                 choices = c("Numbers" = "normal",
                                             "Percentage" = "percent"),
                                 selected = "percent",
                                 multiple = FALSE)
                 )
               )
               
               
               ,fluidPage(DT::dataTableOutput('display_cummulative_dt'), width = "100%")
                 
               ,hr()
               ,h3("Simulation Study")
               ,fluidRow(
                 box(width = 12,
                     highchartOutput("simulation_scatter_hc"))
               )
               
               
               
               
      )
      
      # 
      # tabItem(tabName = "spike",
      #         h2("Spike results"), 
      #         
      #         
      #         fluidRow(
      #           #highcharts goes here
      #           box(width = 3,
      #               selectInput("SITE_ID", label="Select Country", 
      #                           choices = c("UK", "DE"),
      #                           selected = "UK",
      #                           multiple = FALSE)
      #           ),
      #           box(width = 3,
      #               selectInput("TVCampaign", label="Select Campaign name",
      #                           choices = unique(unlist(SPIKE$Campaign_Name)),
      #                           selected = unique(unlist(SPIKE$Campaign_Name))[1])
      #           ),
      #           box(width = 3,
      #               selectInput("TVChannel", label="Select Channel",
      #                           choices = unique(unlist(SPIKE$Channel)),
      #                           selected = unique(unlist(SPIKE$Channel)),
      #                           multiple = TRUE)
      #           ),
      #           box(width = 3,
      #               selectInput("TVMetric", label="Select Metric",
      #                           choices = unique(unlist(SPIKE$Metric)),
      #                           selected = "sessions",
      #                           multiple = FALSE)
      #           )
      #         )
      #         
      #         
      #         
      #         
      #         fluidPage(DT::dataTableOutput('spike'))
      # )
      
      
      
      
    )
  )
)