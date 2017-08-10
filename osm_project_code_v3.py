
# coding: utf-8

# In[1]:

import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint
from xml.etree.ElementTree import parse
from unidecode import unidecode
import csv
import sqlite3
from odo import discover, resource, odo


# In[2]:

# Reference to Data Auditing process (project_submission_doc.pdf)
# 1) Provide a programmable reference to correct data
## REF1
import normalized_stations


# In[3]:

# Reference to Data Auditing process (project_submission_doc.pdf)
# 2) Sample the OSM file and clean station names
actual_stations = []
decoded_stations = {}

## REF2
def parse_and_collect_nodes(osmfile):
    osm_file = open(osmfile, "r")
    doc = ET.iterparse(osm_file, events=("start","end"))
    try:
        for event, elem in doc:
            if event == "start" and elem.tag == "node":
                process_every_node(elem.iter("tag"))
            elif event == "end":
                continue
    except:
        print "End of file reached"
## REF3
def process_every_node(itero):
    k_tags = []
    v_tags = []
    for tag in itero:
        k_tags.append(tag.attrib['k']) 
        v_tags.append(tag.attrib['v'])
    extract_stations(k_tags, v_tags)  

## REF4
def extract_stations(k_tags, v_tags):
    if "subway" in k_tags:
        actual_stations.append(v_tags[0])

## REF5
def to_ascii_actual_stations():
    for index, station in enumerate(actual_stations):
        # decoded_stations.append(unidecode(station))
        decoded_stations[index] = unidecode(station)


# In[4]:

# Reference to Data Auditing process (project_submission_doc.pdf)
# 3) First pass stations and the rest and 4) Dealing with the “no match” stations names
match_stations   = {}
nomatch_stations = {}
dict_of_tokeners = {}
final_review_nomatch = []

## REF7

tokens_to_ignore = [" de ", " los ", "-", " La ", " Los ", "los", "\(E\)", "\(C\)", "\(D\)"]
## split each station name in final_normalization in tokens ignoring the ones in tokens_to_ignore
pattern = '|'.join(tokens_to_ignore)

# first pass of the actual_stations collected from OSM through a compare with official stations
## REF6
def split_match_nomatch_stations():
    for key in decoded_stations.keys():
        if decoded_stations[key] in normalized_stations.all_stations():
            # print "found : {}, index: {}".format(decoded_stations[key],key)
            match_stations[key] = decoded_stations[key]
        else:
            nomatch_stations[key] = decoded_stations[key]

def split_nomatch():
    for key in nomatch_stations.keys():        
        tokeners = re.sub(pattern, ' ', nomatch_stations[key])
        dict_of_tokeners[key] = tokeners

def review_nomatch(tokener, key):
    nomatch_dict_key = key
    tokis = tokener.split()
    match_counter = defaultdict(int)
    for station in normalized_stations.all_stations():
        for tok in tokis:
            if tok in station:
                match_counter[station] += 1
                datum = {"nomatch_key" : nomatch_dict_key, "norm_station" : station, "nomatch_station" : nomatch_stations[nomatch_dict_key], "count" : match_counter[station]}
                final_review_nomatch.append(datum)



# In[5]:

OSMFILE = "bs_as_subway.osm"


# In[ ]:

# Auditing process execution


# In[6]:

parse_and_collect_nodes(OSMFILE)
# calls process_every_node()
# --> calls extract_stations


# In[7]:

to_ascii_actual_stations()
split_match_nomatch_stations()
split_nomatch()


# In[8]:

for key in dict_of_tokeners.keys():
    review_nomatch(dict_of_tokeners[key], key)


# In[ ]:

## 4) Dealing with the “no match” stations names


# In[196]:

# Start building dicts to export to csv and to sqlite3
# stations that did not match
nomatch_stations_dict_list = []
for dat in nomatch_stations.keys():
    datum = {"station_id" : dat, "station_name" : nomatch_stations[dat], "match" : "no"}
    nomatch_stations_dict_list.append(datum)


# In[197]:

# stations that did match
match_stations_dict_list = []
for dat in match_stations.keys():
    datum = {"station_id" : dat, "station_name" : match_stations[dat], "match" : "yes"}
    match_stations_dict_list.append(datum)


# In[198]:

# The sum of match and nomatch stations decoded
decoded_stations_dict_list = []
for dat in decoded_stations.keys():
    datum = {"station_id" : dat, "station_name" : decoded_stations[dat]}
    decoded_stations_dict_list.append(datum)


# In[199]:

# The official list of stations and lines
all_stations_dict_list = []

def process_stations_by_lines(line, stations):
    for station in stations:
        dico = {"station_name"  : station, "line" : line}
        all_stations_dict_list.append(dico)


lines = ["A","B","C","D","E","H","P"]
for line in lines:
    stations = []
    stations = normalized_stations.stations_by_line(line)
    process_stations_by_lines(line, stations)


# In[200]:

# The csv files to export  to sqlite3
## REF8

NOMATCH_STATIONS_TOKENS_PATH = "nomatch_tokens.csv" # final_review_nomatch
NOMATCH_STATIONS_PATH        = "nomatch.csv" # nomatch_stations_dict_list
MATCH_STATIONS_PATH          = "match.csv"   # match_stations
DECODED_STATIONS_PATH        = "decoded.csv" # decoded_stations
OFICIAL_STATIONS_PATH        = "official.csv" # all_stations_dict_list


# In[201]:

# Fields for tables
NOMATCH_STATIONS_TOKENS_FIELDS = ['count','norm_station','nomatch_station','nomatch_key'] # final_review_nomatch
NOMATCH_STATIONS_PATH_FIELDS   = ['station_name','match','station_id'] # nomatch_stations
MATCH_STATIONS_PATH_FIELDS     = ['station_name','match','station_id']   # match_stations
DECODED_STATIONS_PATH_FIELDS   = ['station_name','station_id'] # decoded_stations
OFICIAL_STATIONS_PATH_FIELDS   = ['station_name','line'] # all_stations_dict_list


# In[202]:

# csv files creation function

def create_csv_files(path, fields, dic):
    with open(path, 'w') as csvfile:
        fieldnames = fields
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(dic)


# In[203]:

create_csv_files(OFICIAL_STATIONS_PATH, OFICIAL_STATIONS_PATH_FIELDS, all_stations_dict_list)
create_csv_files(NOMATCH_STATIONS_TOKENS_PATH, NOMATCH_STATIONS_TOKENS_FIELDS, final_review_nomatch)
create_csv_files(NOMATCH_STATIONS_PATH, NOMATCH_STATIONS_PATH_FIELDS, nomatch_stations_dict_list)
create_csv_files(MATCH_STATIONS_PATH, MATCH_STATIONS_PATH_FIELDS, match_stations_dict_list)
create_csv_files(DECODED_STATIONS_PATH, DECODED_STATIONS_PATH_FIELDS, decoded_stations_dict_list)


# In[204]:

# OK, now its time to build the db and the tables
conn = sqlite3.connect('subway.db')
c = conn.cursor()
c.execute('''create table nomatch_tokens (count integer, norm_station text, nomatch_station text, nomatch_key integer)''')
c.execute('''create table nomatch_stations (station_name text, match text, station_id integer)''')
c.execute('''create table match_stations (station_name text, match text, station_id integer)''')
c.execute('''create table decoded_stations (station_name text, station_id integer)''')
c.execute('''create table official_stations (station_name text, line text)''')


# In[205]:

# csv to db 
def csv_to_db(csv_file, table_name, db_con='subway.db'):
    dshape = discover(resource(csv_file))
    uri = 'sqlite:///' + db_con + '::' + table_name
    try :
        odo(csv_file, uri, dshape=dshape)
        print "Data loaded"
    except:
        print "Problems loading data"



# In[206]:

csv_to_db(NOMATCH_STATIONS_TOKENS_PATH, 'nomatch_tokens')
csv_to_db(NOMATCH_STATIONS_PATH, 'nomatch_stations')
csv_to_db(MATCH_STATIONS_PATH , 'match_stations')
csv_to_db(DECODED_STATIONS_PATH, 'decoded_stations')
csv_to_db(OFICIAL_STATIONS_PATH , 'official_stations')


# In[207]:

# SQL query to find out which no matching tokenized station name is the best candidate
## REF9

QUERY = '''
select nomatch_tokens.nomatch_station as normalizar,
count(nomatch_tokens.norm_station) as match,
official_stations.station_name as normal
from nomatch_tokens, official_stations
where nomatch_tokens.norm_station = official_stations.station_name
and nomatch_tokens.nomatch_key = %d
group by nomatch_tokens.norm_station
order by match desc
limit 1;
'''

# find out the ID of the nonmatching stations
NOMATCH_IDS = '''
select distinct nomatch_key from nomatch_tokens;
'''


# In[208]:

# process the list of nonmatchin ids and tokenized versions to list the candidates
candidates_ids = []
candidates_list = []

def search_nomatch_candidates(cursor):
    for nmid in candidates_ids:
        nomatch_id = nmid
        cursor.execute(QUERY%(nomatch_id))
        nomatch_string = cursor.fetchone()
        candidates_list.append(nomatch_string)
    

def search_nomatch_ids(cursor):
    cursor.execute(NOMATCH_IDS)
    results = cursor.fetchall()
    for tup in results:
        candidates_ids.append(tup[0])
        
        
search_nomatch_ids(c)
search_nomatch_candidates(c)
# And finally the list of candidates that match !

for cand in candidates_list:
    print cand
    

