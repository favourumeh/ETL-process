# -*- coding: utf-8 -*-
"""
Created on Sun Aug 22 20:21:22 2021

@author: favou
"""
import pandas as pd


#1) Extract Process 

#CSV Extract Function
def extract_from_csv(csv_file):
    df = pd.read_csv(csv_file)
    return df
    
#JSON Extract Function
def extract_from_json(json_file):
    df = pd.read_json(json_file, lines = True)
    return df

#XML Extract Function
def extract_from_xml(file):
    import xml.etree.ElementTree as ET
    tree = ET.parse(file) #--etree.ElementTree.ElementTree
    root = tree.getroot()
    columns = ["name", "height", "weight"]
    df = pd.DataFrame(columns=columns)
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        series = pd.Series([name,height,weight], index = columns)
        df = df.append(series, ignore_index = True)
    return df

#Complete Extraction Function
def extract():
    columns =["name", "height", "weight"]
    extracted_data = pd.DataFrame(columns = columns)
    current_directory = "C:/Users/favou/Desktop/Summer Python/IBM/ETL"
    
    # To generate a list of all the files we use the glob module
    import glob 
    
    #a) extracting all csv files 
    for csvfile in glob.glob("*.csv"):
        csv_df = extract_from_csv(csvfile)
        extracted_data = extracted_data.append(csv_df, ignore_index = True)

    #b) extracting all json files  
    for jsonfile in glob.glob("*.json"):
        json_df = extract_from_json(jsonfile)
        extracted_data = extracted_data.append(json_df, ignore_index = True)
    
    #c) extracting all xml files
    for xmlfile in glob.glob("*.xml"):
        xml_df = extract_from_xml(xmlfile)
        extracted_data = extracted_data.append(xml_df, ignore_index = True)
        
#note: ignore_index = True ensures that as the dataframes for each file are being...
       #...appended to the 'extracted_data' dataframe the index of each individual...
       #... dataframe is not utilised as the index of the 'extracted_data' dataframe ...
       #... but rather a new index for the 'extracted_data' dataframe is made 
       #... which will count from 0 at the first row to the end   
    
    return extracted_data
    

#2) Transform process (height data: inches to m, weight data: pounds to kg)
def transform(df):
    """ This function converts the height and weight column from inches to m 
        and from pounds to kg. It also rounds the values"""
    df["height"] =round(df.height*0.0254,2)
    df["weight"] = round(df.height*0.45359237, 2)
    return df
    
    
#3) Loading process (the transormed data is saved as a csv)
def load(target_file, transformed_df):
    transformed_df.to_csv(target_file, index=False)

#4) Logging (logging the save process for future reference)
def log(message):
    from datetime import datetime # module allows you to determing the current date
    
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second

    #determining current date as a sting with format: '%Y-%h-%d-%H:%M:%S'
    now = datetime.now() #--datetime
    timestamp =now.strftime(timestamp_format) # strftime = string formated time

    #creating log file 
    with open("logfile1.txt", 'a') as f:
        f.write(timestamp + "," + message + "\n" )


#5) Running ETL process 

def run_ETL():
    log("ETL job started")
    
    log("Extraction Phase started")
    extracted_data = extract()
    log("Extraction Phase finished")
    extracted_data


    log("Transform Phase started")
    transformed_data = transform(extracted_data)
    log("Transform Phase finished")
    transformed_data
    
    
    log("Loading Phase started")
    load("Height_Weight_data1.csv", transformed_data)
    log("Loading Phase finished")
    
    log("ETL job finished")
    
run_ETL()
