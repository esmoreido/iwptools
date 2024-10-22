library(tidyverse)
library(plotly)
library(ggridges)
library(sf)
library(tmap)
source('main.R')

con <- dbcon('moreydo', 'vnFkY9Vj')
dbListTables(con)
dbListFields(con, 'zeyaburea_meteo_forecast')
tmap_mode('view')
# локации станций как слой постгис
tttr_meta <- st_read(con, 'tttr_meta', query = "SELECT * FROM tttr_meta")
tm_shape(tttr_meta) +
  tm_dots(col = 'adm')
q <- 'SELECT index, year, count(temp) nt, count(prec) np, count(def) nd FROM zeyaburea_meteo_fact GROUP BY index, year ORDER BY index, year'
ntttr <- dbGetQuery(con, q)
save(ntttr, file = 'D:/YandexDisk/ИВПРАН/НГМС/отчет2024/ntttr.RData')
load('D:/YandexDisk/ИВПРАН/НГМС/отчет2024/ntttr.RData')
length(unique(ntttr$index))

ntr <- tttr_meta %>%
  full_join(ntttr, by = 'index')
ntr %>%
  group_by(index) %>%
  summarise(mnt = median(nt), 
            min_year = min(year), 
            max_year = max(year)) %>%
  tm_shape() +
  tm_dots(col = 'mnt', popup.vars = c('min_year', 'max_year'))

# p1 <- ntttr %>%
#   pivot_longer(!c(index, year), names_to = 'var', values_to = 'val') %>%
#   ggplot(aes(x=year, y=factor(index), fill=val)) + geom_tile() + 
#   facet_wrap(var~., labeller = as_labeller(c('np'='Осадки', 'nt'='Темепература'))) + 
#   labs(x='Годы', y='Индекс', fill='')
# p1
# 
# p2 <- ntttr %>%
#   pivot_longer(!c(index, year), names_to = 'var', values_to = 'val') %>%
#   ggplot(aes(x=year, y=factor(index), fill=val)) + geom_raster() + facet_wrap(var~.) + 
#   scale_fill_gradient(low = 'blue', high = 'red') 
# p2 
# ggplotly(p2)
# 
# ntttr %>%
#   pivot_longer(!c(index, year), names_to = 'var', values_to = 'val') %>%
#   ggplot(aes(x=year, fill=var)) + geom_density(stat = 'count', alpha = 0.5) + facet_wrap(var~.) + 
#   scale_x_continuous(breaks = seq(1850, 2020, 5), expand = c(0,0)) + 
#   theme(axis.text.x = element_text(angle = 90))
# 
# ntttr %>%
#   ggplot(aes(x=year, y=factor(index))) + geom_density_ridges2(aes(height=np))
# 
# 

q <- "SELECT index, date_part('year', date), count(mean_temp) nt, count(prec) np, count(def) nd, source FROM trd_day WHERE prec IS NOT NULL OR mean_temp IS NOT NULL GROUP BY index, date_part('year', date), source ORDER BY index, date_part('year', date)"
ntrd <- dbGetQuery(con, q)
save(ntrd, file = 'D:/YandexDisk/ИВПРАН/НГМС/отчет2024/ntrd.RData')
load('D:/YandexDisk/ИВПРАН/НГМС/отчет2024/ntrd.RData')
ntrd %>%
  rename('year'='date_part') %>%
  group_by(index, source) %>%
  summarise(miy = min(year),
            may = max(year)) %>%
  group_by(source) %>%
  summarise(start = median(miy),
            end = median(may))
ntrdm <- tttr_meta %>%
  full_join(ntrd, by = 'index')
ntrdm %>%
  group_by(index, name) %>%
  summarise(mnt = median(nt, na.rm = T), 
            min_year = min(date_part, na.rm = T), 
            max_year = max(date_part, na.rm = T)) %>%
  tm_shape() +
  tm_dots(title = 'Температура', col = 'mnt', popup.vars = c('name', 'min_year', 'max_year'))
dbListFields(con, 'gmvo_data')
q <- "SELECT * FROM gmvo_data WHERE variable_id = 2 AND index <> 49001 ORDER BY value ASC LIMIT 100"
df <- dbGetQuery(con, q)

q <- "SELECT index, date_part('year', date), count(value), variable_id FROM gmvo_data GROUP BY index, date_part('year', date), variable_id"
df <- dbGetQuery(con, q)

ggplot(df, aes(x=date_part, y=factor(index), fill=count)) + geom_tile() + 
  facet_wrap(variable_id~., ncol = 1, strip.position = 'right') + 
  scale_x_continuous(breaks = seq(2008,2020,1)) + 
  scale_fill_gradient2(low = 'black', mid = 'yellow', high = 'red')
ggsave(filename = "qh_gmvo05-24.png", width = 18, height = 50, dpi = 150, device = 'png', limitsize = F)

q <- "SELECT * FROM hydr_daily WHERE index IN (76284,76289,76295) AND date BETWEEN ('1900-01-01','2100-12-31') ORDER BY date, index"
df <- dbGetQuery(con, q)

sher_q <- df %>%
  mutate(q = (3.46252991788256 * 10 ^ -6) * (value ^ 3.1435158600594))

summary(sher_q)
getwd()
sher_q %>%
  select(date, q) %>%
  mutate(date = format(date, "%Y%m%d"),
         q = round(q, 1)) %>%
  write_csv(file = 'd:/EcoAmur_BI/Data/Hydro/AMUR/5115.csv', na = '-99', col_names = F)

ggplot(sher_q, aes(x=date, y=q)) + geom_line()

ind <- read_xlsx('d:/maps/НГМС/Амур/Амур 137 постов ГМВО.xlsx')

belaya_data <- get_db_hydro(index = c(76284, 76289, 76295))

belaya_data %>%
  filter(!is.na(value)) %>%
  group_by(index, year = year(date), variable_id) %>%
  summarise(count = n()) %>%
  ggplot(aes(x=year, y=factor(index), fill=count)) + geom_tile() + 
    facet_wrap(variable_id~., ncol = 1, strip.position = 'right') + 
    # scale_x_continuous(breaks = seq(2008,2020,1)) + 
    scale_fill_gradient2(low = 'black', mid = 'yellow', high = 'red')
ggsave(filename = "qh_gmvo_amur.png", width = 18, height = 50, dpi = 150, device = 'png', limitsize = F)

amur_data %>%
  select(date, q) %>%
  mutate(date = format(date, "%Y%m%d"),
         q = round(q, 1)) %>%
  write_csv(file = 'd:/EcoAmur_BI/Data/Hydro/AMUR/5115.csv', na = '-99', col_names = F)

getwd()
dbDisconnect(con)

dir <- 'd:/EcoBelaya/Data/Hydro/Volga/new/Pavs/'
f <- list.files(dir, full.names = T)
q <- data.frame()
for(file in f[-1]){
  print(file)
  df <- read.csv(file, skip = 3, sep = '', header = F, 
                 col.names = c('n', 'date', 'value', 'h'), 
                 colClasses = c('NULL', 'character', 'numeric', 'NULL'), 
                 na.strings = '-99')
  df$date <- as.Date(strptime(df$date, format = '%Y%m%d'))
  df$index <- '76289'
  q <- rbind(q, df)
}
ggplot(q, aes(x=date, y=value)) + geom_line()
setwd(dir)
write_hydr.csv(q)

# Pechora
stations <- c(22292, 22383, 22499, 22583, 23103, 23104, 23105, 23109, 23112, 23114, 23121, 23205, 23207, 23215, 23219, 23220, 23226, 23305, 23311, 23316, 23322, 23326, 23327, 23330, 23331, 23332, 23405, 23411, 23412, 23416, 23418, 23423, 23501, 23503, 23509, 23514, 23518, 23519, 23527, 23606, 23608, 23609, 23624, 23701, 23709, 23711, 23724, 23812, 23813, 23815, 23817, 23827, 23912)

q <- paste0("SELECT index, min(date), max(date) FROM trd_day WHERE index IN (", paste(stations, collapse = ','), ") GROUP BY index ORDER BY index")
q
df <- dbGetQuery(con, q)


q <- "SELECT index, date, def FROM zeyaburea_meteo_fact WHERE date >= '2019-01-01'"
df <- dbGetQuery(con, q)
dbDisconnect(con)

q <- "UPDATE zeyaburea_meteo_fact SET def = NULL WHERE def < 0"
dbExecute(con, q)

dbDisconnect(con)

log1 <- read.pmlog('d:/YandexDisk/ИВПРАН/закупки/2023/логгеры/1159900837820_M1.1_M1.2_Vbatt.csv') 
log2 <- read.pmlog('d:/YandexDisk/ИВПРАН/закупки/2023/логгеры/1159900837520_M1.1_M1.2_Vbatt.csv')

df <- merge(log1, log2, by = 'datetime', suffixes = c('_log1', '_log2'))

ggplot(df, aes(x=temp_log1, y=temp_log2)) + geom_point() + geom_abline()


q <- "SELECT * FROM gmvo_data WHERE variable_id = 1 AND index = 6473 AND date_part('year', date) = 2019 ORDER BY date"
df <- dbGetQuery(con, q)
writexl::write_xlsx(df, path = 'D:/EcoBRH/Data/Hydro/Bure/Mali/2019.xlsx')
