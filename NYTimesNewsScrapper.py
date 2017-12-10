# -*- coding: utf-8 -*-
"""
Created on Sun Dec 10 10:50:43 2017

@author: RemoteDevWork
"""

import json
import datetime
import time
import sys, os
import logging
from configparser import ConfigParser
import requests

# helpful function to figure out what to name individual JSON files        
def getJsonFileName(date, page, json_file_path):
    json_file_name = ".".join([date,str(page),'json'])
    json_file_name = "".join([json_file_path,json_file_name])
    return json_file_name

# Fetch the articles from the NYTimes Article API    
def fetchArticles(date, api_key, json_file_path, number_of_article):
    logging.info("fetchArticles execution started.")
    # LOOP THROUGH THE 50 PAGES NYTIMES ALLOWS FOR THAT DATE based on limit allowed by NY Times to fetch free news article
    for page in range(number_of_article): # number_of_article: 50
        try:
            source_url = 'http://api.nytimes.com/svc/search/v2/articlesearch.json?begin_date={0}&end_date={0}&page={1}&api-key={2}'
            url = source_url.format(date, str(page), api_key)
            json_data = requests.get(url).json()

            if len(json_data["response"]["docs"]) >= 1:
                json_file_name = getJsonFileName(date, page, json_file_path)
                logging.info("fetchArticles json_file_name: %s", json_file_name)

                with open(json_file_name, 'a', encoding='utf-8') as outfile:
                    json.dump(json_data, outfile)
                    logging.info("fetchArticles json_data: %s", json_data)
            # if no more articles, go to next date
            else:
                return
            time.sleep(2) # wait so that fetchArticles method don't overwhelm the API
        except: 
            logging.error("Error on %s: %s", date, sys.exc_info()[0])
            continue

# helpful function for processing keywords, mostly    
def getMultiples(items, key):
    values_list = ""
    if len(items) > 0:
        num_keys = 0
        for item in items:
            if num_keys == 0:
                values_list = item[key]                
            else:
                values_list =  "; ".join([values_list,item[key]])
            num_keys += 1
    return values_list

# parse the JSON files you stored into a tab-delimited file
def parseArticles(date, csv_file_name, json_file_path, number_of_article):
    logging.info("parseArticles execution started.")
    for file_number in range(number_of_article): # parse same number of arlicle 50
        # get the articles and put them into a dictionary
        try:
            file_name = getJsonFileName(date,file_number, json_file_path)
            if os.path.isfile(file_name):
                with open(file_name) as data_file:
                    articles = json.load(data_file)
            else:
                break
        except IOError as e:
            logging.error("IOError in %s page %s: %s %s", date, file_number, e.errno, e.strerror)
            continue

        # if there are articles in that document, parse them
        if len(articles["response"]["docs"]) >= 1:  
            # loop through the articles putting what we need in a tsv   
            try:
                for article in articles["response"]["docs"]:
                    keywords = ""
                    keywords = getMultiples(article["keywords"],"value")
                    variables = [
                        article["pub_date"], 
                        keywords,
                        str(article["headline"]["main"]).replace("\n","") if "main" in article["headline"].keys() else "",
                        str(article["source"]) if "source" in article.keys() else "", 
                        str(article["document_type"]) if "document_type" in article.keys() else "",
                        str(article["web_url"]) if "web_url" in article.keys() else "",
                        str(article["snippet"]).replace("\n","") if "snippet" in article.keys() else "",
                        str(article["lead_paragraph"]).replace("\n","") if "lead_paragraph" in article.keys() else "",
                        str(article["type_of_material"]).replace("\n","") if "type_of_material" in article.keys() else "",
                        str(article["section_name"]).replace("\n","") if "section_name" in article.keys() else ""
                        ]
                    line = "\t".join(filter(None, variables))
                    logging.info("parseArticles line: %s", line)

                    # open the tsv for appending
                    with open(csv_file_name, 'a') as csvfile:
                        csvfile.write(line)
                        csvfile.write("\n")
            except KeyError as e:
                logging.error("KeyError in %s page %s: %s %s", date, file_number, e.errno, e.strerror)
                continue
            except (KeyboardInterrupt, SystemExit):
                raise
            except: 
                logging.error("Error on %s page %s: %s", date, file_number, sys.exc_info()[0])
                continue
        else:
            break

# helper function to iterate through dates
def daterange( start_date, end_date ):
    if start_date <= end_date:
        for n in range( ( end_date - start_date ).days + 1 ):
            yield start_date + datetime.timedelta( n )
    else:
        for n in range( ( start_date - end_date ).days + 1 ):
            yield start_date - datetime.timedelta( n )

# executeNYTimes - Main method 
def executeNYTimes():

    config = ConfigParser()
    script_dir = os.path.dirname(__file__)
    config_file = os.path.join(script_dir, 'config/nytimesConfig.cfg')
    config.read(config_file)

    api_key = config.get('nytimesConfig','api_key')    

    start = datetime.date( year = int(config.get('nytimesConfig','start_year')), month = int(config.get('nytimesConfig','start_month')), day = int(config.get('nytimesConfig','start_day')) )
    end = datetime.date( year = int(config.get('nytimesConfig','end_year')), month = int(config.get('nytimesConfig','end_month')), day = int(config.get('nytimesConfig','end_day')) )

    json_file_path = config.get('generatedData','json_path')
    csv_file_name = config.get('generatedData', 'output_file')
    log_file = config.get('logPath','logfile')
        
    logging.basicConfig(filename=log_file, level=logging.INFO)
    logging.info("ExecuteNYTimes process started.")

    try:
        fieldsnames = ['pub_date', 'snippet', 'headline', 'source', 'document_type', 'web_url', 'lead_paragraph', 'type_of_material', 'section_name']
        headers = "\t".join(fieldsnames)
        with open(csv_file_name, 'a') as csvfile:
            csvfile.write(headers)
            csvfile.write("\n")

        # Fetch Data from NYTimes for date range between start date to end date.
        for date in daterange( start, end ):
            date = date.strftime("%Y%m%d")
            # LOOP THROUGH THE PAGES NYTIMES ALLOWS FOR THAT DATE based on limit allowed by NY Times to fetch free news article
            number_of_article = 50
            logging.info("Executing NY Times News Scrapper for date: %s." % date)
            fetchArticles(date, api_key, json_file_path, number_of_article)
            parseArticles(date, csv_file_name, json_file_path, number_of_article)

    except:
        logging.error("Unexpected error: %s", str(sys.exc_info()[0]))
    finally:
        logging.info("ExecuteNYTimes process completed.")

if __name__ == '__main__' :
    executeNYTimes()