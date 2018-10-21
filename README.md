# American baby names

Python 2.x script that downloads and parses the raw files from the [SSA website](http://www.ssa.gov/oact/babynames/limits.html) of the American Government. The script also creates (if needed) and populates a [Postgres](https://www.postgresql.org) data base with the american baby names registration log over the years.

## Table structure

**states**

| field | description |
| --- | --- |
| st_abbr | State's abbreviation code |
| st_name | Stats's name |

**names**

| field | description |
| --- | --- |
| nm_code | Name's code |
| nm_label | Name |

**registries**

| field | description |
| --- | --- |
| nm_code | Name's code |
| sex_code | Sex's code |
| yob | Year of birth |
| st_abbr | State's abbreviation code |
| total | Total of registries |
