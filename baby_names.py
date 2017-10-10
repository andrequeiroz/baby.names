#!/usr/bin/python2

import csv
import os
import psycopg2
import re
import urllib
import zipfile

# variables
db_name = "baby_names"
wrk_dir = "/tmp/"
names_link = "https://www.ssa.gov/oact/babynames/names.zip"
namesbystate_link = "https://www.ssa.gov/oact/babynames/state/namesbystate.zip"

# connect to baby_names db (create it if needed)
try:
    link = psycopg2.connect(dbname = db_name)
except:
    link = psycopg2.connect(dbname = "postgres")
    link.autocommit = True
    cursor = link.cursor()
    cursor.execute("CREATE DATABASE " + db_name)
    link.close()
    link = psycopg2.connect(dbname = db_name)
    os.system("psql " + db_name + " < ./sql/names.sql")
    os.system("psql " + db_name + " < ./sql/states.sql")
    os.system("psql " + db_name + " < ./sql/sexes.sql")
    os.system("psql " + db_name + " < ./sql/registries.sql")

cursor = link.cursor()

# update existing db check
ans = ""
cursor.execute("SELECT * FROM names")
if len(cursor.fetchall()) > 0:
    while ans not in ["y", "n"]:
        ans = raw_input("update existing db? [y/n]: ")
        if ans == "n":
            quit()
        else:
            cursor.execute("TRUNCATE TABLE registries")
            cursor.execute("TRUNCATE TABLE names RESTART IDENTITY")
            link.commit()

# download raw files (if needed)
if not os.path.isfile(wrk_dir + "names.zip"):
    print "downloading names.zip"
    urllib.urlretrieve(names_link, wrk_dir + "names.zip")

if not os.path.isfile(wrk_dir + "namesbystate.zip"):
    print "downloading namesbystate.zip"
    urllib.urlretrieve(namesbystate_link, wrk_dir + "namesbystate.zip")

# extracting files routine
def extract_files(f):
    zf = open(f, "rb")
    z = zipfile.ZipFile(zf)
    for archive in z.namelist():
        if not archive.endswith(".pdf"):
            z.extract(archive, wrk_dir + "bn/")
    zf.close()

extract_files("/tmp/names.zip")
extract_files("/tmp/namesbystate.zip")

# data parsing
names = []

# order: name, sex, year, state, count
state_resgistries = []

# order: name, sex, year, count
year_registries = []

print "parsing data"
for archive in os.listdir(wrk_dir + "bn/"):
    with open(wrk_dir + "bn/" + archive, "r") as a:
        lines = csv.reader(a)
        if archive.startswith("yob"):
            year = re.search(".+([0-9]{4}).+", archive).group(1)
            for line in lines:
                names.append(line[0])
                year_registries.append([line[0], line[1], year, line[2]])
        else:
            order = [3, 1, 2, 0, 4]
            for line in lines:
                names.append(line[3])
                state_resgistries.append([line[i] for i in order])

# unique names list
names = sorted(list(set(names)))

# populate names table
print "populating names table - pt I"
for name in names:
    query = "INSERT INTO names (nm_label) VALUES ('%s')" % name
    cursor.execute(query)

link.commit()

cursor.execute("SELECT nm_label, nm_code FROM names")
names = dict(cursor.fetchall())

# populate registries table
print "populating registries table"
for registry in state_resgistries:
    registry[0] = names.get(registry[0])
    query = ("INSERT INTO registries VALUES (%s, '%s', %s, '%s', %s)" %
             tuple(registry))
    cursor.execute(query)

link.commit()

# calculation of the difference between total registries and registry by state
print "populating names table - pt II"
for registry in year_registries:
    registry[0] = names.get(registry[0])
    query_select = ("""SELECT sum(total)
                       FROM registries
                       WHERE nm_code = %s AND sex_code = '%s'
                        AND yob = %s""" % tuple(registry[:-1]))
    cursor.execute(query_select)
    value = cursor.fetchall()[0][0]
    if value is not None:
        if value < int(registry[3]):
            registry[3] = int(registry[3]) - value
            query = ("INSERT INTO registries VALUES (%s, '%s', %s, NULL, %s)" %
                     tuple(registry))
            cursor.execute(query)
    elif int(registry[3]) > 0:
        query = ("INSERT INTO registries VALUES (%s, '%s', %s, NULL, %s)" %
                 tuple(registry))
        cursor.execute(query)

link.commit()

# end
cursor.close()
link.close()
