library(RPostgreSQL)
library(dplyr)
library(ggplot2)

link <- dbConnect(dbDriver("PostgreSQL"), dbname = "baby_names")

abs_freq_map <- function(name, sex) {
    name <- deparse(substitute(name))
    sex <- deparse(substitute(sex))

    paste0("SELECT LOWER(s.st_abbr) AS state, SUM(r.total) AS total
            FROM registries AS r
             INNER JOIN names AS n ON r.nm_code = n.nm_code
             INNER JOIN states AS s ON r.st_abbr = s.st_abbr
            WHERE r.st_abbr <> 'DC'
             AND n.nm_label = '", name, "' AND r.sex_code = '", sex, "'
            GROUP BY s.st_abbr") %>%
        dbGetQuery(link, .) %>%
        right_join(map_data("state"), by = c("state" = "region")) %>%
        ggplot() +
        geom_polygon(mapping = aes(x = long, y = lat,
                                   group = group, fill = total),
                     colour = "black", size = 0.2) +
        scale_fill_gradient(low = "red", high = "green", na.value = NA,
                            labels = function(x) format(x, big.mark = ".",
                                                        decimal.mark = ",",
                                                        scientific = FALSE)) +
        coord_fixed(1.7) +
        theme(title = element_blank(), axis.text = element_blank(),
              axis.ticks = element_blank())
}

rel_freq_map <- function(name, sex) {
    name <- deparse(substitute(name))
    sex <- deparse(substitute(sex))

    paste0("SELECT res.state_f AS state, res.freq::numeric / res.total AS prop
            FROM ((SELECT LOWER(s.st_abbr) AS state_f, SUM(r.total) AS freq
                   FROM registries AS r
                    INNER JOIN names AS n ON r.nm_code = n.nm_code
                    INNER JOIN state AS s ON r.st_abbr = s.st_abbr
                   WHERE r.st_abbr <> 'DC'
                    AND n.nm_label = '", name, "' AND r.sex_code = '", sex, "'
                   GROUP BY s.st_abbr) AS filter
                   INNER JOIN (SELECT LOWER(s.st_abbr) AS state_t,
                                SUM(r.total) AS total
                               FROM registries AS r
                                INNER JOIN state AS s ON r.st_abbr = s.st_abbr
                               WHERE r.st_abbr <> 'DC'
                                AND r.sex_code ='", sex, "'
                               GROUP BY s.st_abbr) AS total
                    ON filter.state_f = total.state_t) AS res") %>%
        dbGetQuery(link, .) %>%
        right_join(map_data("state"), by = c("state" = "region")) %>%
        ggplot() +
        geom_polygon(mapping = aes(x = long, y = lat,
                                   group = group, fill = prop),
                     colour = "black", size = 0.2) +
        scale_fill_gradient(low = "red", high = "green", na.value = NA,
                            labels = function(x) format(x, big.mark = ".",
                                                        decimal.mark = ",",
                                                        scientific = FALSE)) +
        coord_fixed(1.7) +
        theme(title = element_blank(), axis.text = element_blank(),
              axis.ticks = element_blank())
}

abs_freq_map(Alexa, F)
rel_freq_map(Alexa, F)

"SELECT s.st_abbr, n.nm_label, SUM(r.total) AS total
 FROM registries AS r
  INNER JOIN names AS n ON r.nm_code = n.nm_code
  INNER JOIN state AS s ON r.st_abbr = s.st_abbr
 WHERE r.sex_code = 'F'
 GROUP BY s.st_abbr, n.nm_label
 ORDER BY s.st_abbr, total DESC
 LIMIT 10" %>%
    dbGetQuery(link, .)

"SELECT r.yob, r.sex_code AS sex, SUM(r.total) AS total
 FROM registries AS r
  INNER JOIN names AS n ON r.nm_code = n.nm_code
 WHERE n.nm_label = 'Alexa'
 GROUP BY r.yob, r.sex_code
 ORDER BY r.yob" %>%
    dbGetQuery(link, .) %>%
    ggplot() +
    geom_line(mapping = aes(x = yob, y = total, colour = sex)) +
    scale_x_continuous(limits = c(1880, 2016))

dbDisconnect(link)
