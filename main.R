# pkgs <- c("devtools", "roxygen2", "usethis")
# install.packages(pkgs)
library(devtools)
library(roxygen2)
library(usethis)
library(readxl)
library(lubridate)
library(dplyr)
library(tidyverse)
library(RPostgreSQL)
# create_package("iwptools")

# соединение с базой 203 ----
dbcon <- function(login, pwd){
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, dbname = "hydromet",
                   host = "192.168.5.203", port = 5432,
                   user = login, password = pwd)
  return(con)
}

# соединение с базой 215 ----
dbcon215 <- function(login, pwd){
  con <- DBI::dbConnect(odbc::odbc(), 
                        Driver = "SQL Server",
                        Server = "192.168.5.215",
                        Database = "amur_gdm",
                        UID = login,
                        PWD = pwd,
                        # Trusted_Connection = "Yes",
                        Port = 1433,
                        applicationIntent = "readonly")
  # dbConnect(drv, dbname = "amurhdm",
  #                  host = "192.168.5.215", port = 1433,
  #                  user = login, password = pwd)
  return(con)
}

# соединение с базой 219 ----
dbcon219 <- function(login, pwd){
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, dbname = "amur22_non_iwp",
                   host = "192.168.5.219", port = 5432,
                   user = login, password = pwd)
  return(con)
}

# запрос базы на метео из ВНИИГМИ по индексам постов и периоду дат ----


# запрос из базы списка индексов и названий постов с мин и макс датой ----
get_db_hydro_inventory <- function(){
  con <- dbcon(login = "moreydo", pwd = "vnFkY9Vj")
  qry <- paste0("SELECT DISTINCT h.index as index, m.namefull as name, m.basin as basin, min(h.date) as date_start, max(date) as date_end, count(q) as nq, count(h) as nh FROM hydr_daily h LEFT JOIN hydr_meta m ON h.index = m.index GROUP BY h.index, m.namefull, m.basin ORDER BY h.index")
  qry
  df <- dbGetQuery(con, qry)
  unique(df$index)
  return(df)
}


# запрос базы на гидро из hydr_daily по индексам постов и периоду дат ----
get_db_hydro <- function(index, date_start = '1900-01-01', date_end = '2100-12-31'){
  con <- dbcon(login = "moreydo", pwd = "vnFkY9Vj")
  qry <- paste0("SELECT * FROM hydr_daily WHERE index IN (", paste0(index, collapse = ','), ") AND date BETWEEN (", paste0('\'',date_start, '\',', '\'', date_end,'\''), ') ORDER BY date, index')
  qry
  df <- dbGetQuery(con, qry)
  unique(df$index)
  return(df)
}

# запрос базы на гидро из ГМВО по индексам постов и периоду дат ----
# pwr <- "vnFkY9Vj"
# login <- "moreydo"
# index <- hp$index
# type <- 2
#' Title
#'
#' @param index 
#' @param date_start 
#' @param date_end 
#' @param type 
#'
#' @return
#' @export
#'
#' @examples
get_gmvo_data <- function(index, type){
  con <- dbcon(login = "moreydo", pwd = "vnFkY9Vj")
  qry <- paste0("SELECT * FROM gmvo_data WHERE index IN (", paste0(index, collapse = ','), ") AND variable_id IN (", paste0(type, collapse = ','), ") ORDER BY index, date")
  print(qry)
  df <- dbGetQuery(con, qry)
  unique(df$index)
  return(df)
}

# метео из базы данных по списку постов и дат ----
get_meteo_data_daily <- function(index, date_start, date_end){
  con <- dbcon(login = "moreydo", pwd = "vnFkY9Vj")
  qry <- paste0("SELECT index, date, mean_temp, prec, def FROM trd_day WHERE index IN (", paste0(index, collapse = ','), ") AND date BETWEEN '", date_start, "' AND '", date_end, "' ORDER BY index, date")
  qry
  df <- dbGetQuery(con, qry)
  unique(df$index)
  return(df)
}


# парсинг страницы с pogodaiklimat.ru по индексу и дате. сохраняются температура воздуха и точки росы, осадки и давление ----
parse_pik <- function(url, ind, yr){
  # url <- 'http://www.pogodaiklimat.ru/weather.php?id=27515&bday=1&fday=30&amonth=4&ayear=2021&bot=2'
  tables <- html_nodes(read_html(url), "table")  # парсим по тэгу
  tbl1 <- html_table(tables[[1]], header = F) # список из двух таблиц, в одной - сроки и дата, во второй - данные ??\_(???)_/??
  tbl2 <- html_table(tables[[2]], header = F) 
  
  tbl1 <- tbl1[-1,] # избавляемся от ненужных хэдеров
  tbl2 <- tbl2[-1,]
  # ind <- 27515
  # записываем в фрейм
  df <- data.frame(index = ind, 
                   time = as.numeric(tbl1$X1), 
                   daymon = as.character(tbl1$X2), 
                   temp = as.numeric(tbl2$X6), 
                   prec = as.numeric(tbl2$X16), 
                   td = as.numeric(tbl2$X7),
                   pres = as.numeric(tbl2$X13))
  
  df$month <- as.numeric(substr(df$daymon, start = stri_locate_first(df$daymon, fixed = ".") + 1, 
                                stop = stri_locate_first(df$daymon, fixed = ".") + 3)) # делаем месяц
  df$day <- as.numeric(substr(df$daymon, start = 1, stop = stri_locate_first(df$daymon, fixed = ".") - 1)) # делаем день
  
  df$date <- make_datetime(year = yr, month = df$month, day = df$day, hour = df$time) # делаем дату
  # оставляем только нужные столбцы
  df <- df %>%
    dplyr::select(index, date, temp, prec, td, pres)
  # print(head(df))
  return(df)
}

# скачивание данных с pogodaiklimat.ru по индексам постов и датам начала и конца ----


# импорт данных с логгеров HOBO - можно csv и xlsx ----
#' Title
#'
#' @param filename путь до входного файла
#' @param station_name название станции
#' @param data_type тип прибора: 1 - уровень, температура, 2 - электропроводность, температура
#' @param ninecol в файле 9 столбцов?
#' @param hobo_xls входной файл формата xlsx?
#' @param hobo_header Есть первая строка с номером (Plot Title: XXXXXXX)?
#'
#' @return csv файл с результатами
#' @export
#'
#' @examples
read.hobo <- function(filename, station_name = '', data_type = 1, 
                      ninecol = FALSE, hobo_xls = TRUE, hobo_header = FALSE) {
  var_name <- data.frame(id = c(42,43,44,45), 
                         var_name = c("water_temp_u20", "water_temp_u24", 
                                      "water_pres_u20", "water_cond_u24"))
  if(data_type == 1){
    var_name <- var_name %>%
      filter(grepl('u20', var_name))
  }else{
    var_name <- var_name %>%
      filter(grepl('u24', var_name))
  }
  
  if(ninecol == FALSE){
    coln <- c('N', 'datetime', var_name$var_name[2], var_name$var_name[1], 'cd', 'ca', 'hc', 'eof')
    colt <- c('numeric', 'date', rep('text', 6))
  }else{
    colt <- c('numeric', 'date', rep('text', 7))
    coln <- c('N', 'datetime', var_name$var_name[2], var_name$var_name[1], 'cd', 'ca', 'hc', 'std', 'eof')
  }
  
  if(hobo_header == TRUE){
    skipl <- 2
  }else{
    skipl <- 1
  }
  
  if(hobo_xls == FALSE){
    hobo_df <- read.csv(file = filename, 
                        sep = ',',  encoding = 'UTF-8', header = F, skip = skipl,
                        col.names = coln)
    hobo_df <- hobo_df %>%
      mutate(datetime = as.POSIXct(strptime(datetime, format = '%m.%d.%y %I:%M:%S %p')),
             station_name = station_name)
  }else if (hobo_xls == TRUE){
    hobo_df <- read_xlsx(path = filename, skip = skipl, 
                         col_types = colt,
                         col_names = coln)
    print(head(hobo_df))
    hobo_df <- hobo_df %>%
      mutate(station_name = station_name)
  }
  print(head(hobo_df))
  
  # hobo_df <- hobo_df %>%
  #   select(datetime, station_name, !!as.name(var_name$var_name[2]), !!as.name(var_name$var_name[1]))
  # 
  return(hobo_df)
}


# функция для чтения данных с логгеров Promodem ----
#' Title
#'
#' @param datafile 
#'
#' @return
#' @export
#'
#' @examples
read.pmlog <- function(datafile) {
  df <- read.csv(datafile, sep = ';', 
                          encoding = 'UTF-8', check.names = F, dec = ',', 
                          col.names = c('datetime', 'pres', 'pres.unit', 
                                        'temp', 'temp.unit', 'vbatt', 'vbatt.unit'))
  df <- df %>%
    filter(!is.na(pres)) %>%
    mutate(datetime = as.POSIXct(strptime(datetime, format = "%d.%m.%Y %H:%M:%S"))) %>%
    dplyr::select(datetime, pres, temp)
  
  return(df)
}


# загрузка из формата выдачи нового ECOMAG ----
#' Title
#'
#' @param filename путь до файла '/SimOut/Hdrs.csv'
#'
#' @return
#' @export
#'
#' @examples
read.simout <- function(filename){
  df <- read.csv(filename, check.names = F, stringsAsFactors = F)
  
  df <- df %>%
    mutate(Date = as.Date(strptime(as.character(Date), format = '%Y%m%d'))) %>%
    pivot_longer(!Date, names_to = 'var', values_to = 'val') %>%
    mutate(post = gsub(pattern = "\\_.+", replacement = "", x = var), 
           calc = sub('.*(?=.$)', '', var, perl=T)) %>%
    ggplot(aes(x=Date, y=val, col=calc)) + 
    geom_line() + 
    facet_wrap(post~., scales = 'free_y')
  return(df)
  
}


# загрузка из формата keller xlsx ----
#' Title
#'
#' @param filename путь до файла xlsx KolibriDesktop
#'
#' @return
#' @export
#'
#' @examples
read.keller <- function(filename){
  keller <- readxl::read_xlsx(filename, 
                              range = 'C11:E12066', col_names = c('datetime', 'pres_keller', 'temp_keller'))
  keller <- keller %>% 
    mutate(datetime = with_tz(datetime, tzone = 'Europe/Moscow'), 
           pres_keller = pres_keller * 100)
}


# формирование файлов гидрологии csv для новой версии ECOMAG ----
write_hydr.csv <- function(x){
  print(head(x))
  filename <- paste0(unique(x$index), '.csv')
  print(filename)
  x %>%
    dplyr::select(date, value) %>%
    mutate(date = strftime(date, "%Y%m%d")) %>%
    write.table(file = filename, quote = F, col.names = F,
                row.names = F, sep = ",", na = "-99")
}
getwd()

# amur_data %>%
#   filter(variable_id == 2) %>%
#   select(index, date, value) %>%
#   group_by(index) %>%
#   group_walk(~ write_hydr.csv(.x), .keep = T)
# 
# 
# df <- get_gmvo_data(6128, 2)
# ggplot(df, aes(x=date, y=value)) + geom_line()
