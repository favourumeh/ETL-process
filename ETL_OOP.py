# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 12:20:49 2021

@author: favou
"""
import pandas as pd


class ETL_oop:
    def __init__(self, logfile, target_file_directory, run = 'No'):
        self.logfile = logfile
        self.target_file_directory = target_file_directory
        self.run = run
    
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
    def extract(self):
        columns =["name", "height", "weight"]
        extracted_data = pd.DataFrame(columns = columns)
        
        # To generate a list of all the files we use the glob module
        import glob 
        
        #a) extracting all csv files 
        for csvfile in glob.glob("*.csv"):
            csv_df = ETL_oop.extract_from_csv(csvfile)
            extracted_data = extracted_data.append(csv_df, ignore_index = True)
    
        #b) extracting all json files  
        for jsonfile in glob.glob("*.json"):
            json_df = ETL_oop.extract_from_json(jsonfile)
            extracted_data = extracted_data.append(json_df, ignore_index = True)
        
        #c) extracting all xml files
        for xmlfile in glob.glob("*.xml"):
            xml_df = ETL_oop.extract_from_xml(xmlfile)
            extracted_data = extracted_data.append(xml_df, ignore_index = True)
            
    #note: ignore_index = True ensures that as the dataframes for each file are being...
           #...appended to the 'extracted_data' dataframe the index of each individual...
           #... dataframe is not utilised as the index of the 'extracted_data' dataframe ...
           #... but rather a new index for the 'extracted_data' dataframe is made 
           #... which will count from 0 at the first row to the end   
        
        self.extracted_data = extracted_data
    

#2) Transform process (height data: inches to m, weight data: pounds to kg)
    def transform(self):
        if self.run == 'No':
            call = ETL_oop.extract(self)
            
        """ This function converts the height and weight column from inches to m 
            and from pounds to kg. It also rounds the values"""
        self.extracted_data["height"] =round(self.extracted_data.height*0.0254,2)
        self.extracted_data["weight"] = round(self.extracted_data.height*0.45359237, 2)
        self.transformed_data = self.extracted_data[:]
    
    
#3) Loading process (the transormed data is saved as a csv)
    def load(self):
        if self.run == 'No':
            call = ETL_oop.transform(self)
            
        self.transformed_data.to_csv(self.target_file_directory, index = False)

#4) Logging (logging the save process for future reference)
    def log(self, message):
        from datetime import datetime # module allows you to determing the current date
        
        timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    
        #determining current date as a sting with format: '%Y-%h-%d-%H:%M:%S'
        now = datetime.now() #--datetime
        timestamp =now.strftime(timestamp_format) # strftime = string formated time
    
        #creating log file 
        with open(logfile, 'a') as f:
            f.write(timestamp + "," + message + "\n" )


#5) Running ETL process 

    def run_ETL(self):
        self.run = 'Yes'
        
        
        ETL_oop.log(self, "ETL job started")
        
        ETL_oop.log(self, "Extraction Phase started")
        extracted_data = ETL_oop.extract(self)
        ETL_oop.log(self,"Extraction Phase finished")
        extracted_data
    
    
        ETL_oop.log(self, "Transform Phase started")
        transformed_data = ETL_oop.transform(self)
        ETL_oop.log(self, "Transform Phase finished")
        transformed_data
        
        
        ETL_oop.log(self, "Loading Phase started")
        ETL_oop.load(self)
        ETL_oop.log(self, "Loading Phase finished")
        
        ETL_oop.log(self, "ETL job finished")
        
        
        

#Testing 
logfile = "logfile.txt"
target_file_directory = "Height_Weight_data.csv"
A = ETL_oop(logfile, target_file_directory)
A.run_ETL()
