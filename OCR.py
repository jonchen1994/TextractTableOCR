# Last updated 3/14/2025

import boto3
import os
from tqdm import tqdm
from dotenv import load_dotenv
import pandas as pd
# from textractor import Textractor
# from textractor.data.constants import TextractFeatures
from Common_Functions import *
from OCR_Functions import *
import time
import pickle
import io


load_dotenv()

# AWS credentials

client = boto3.client("s3", 
                      region_name = os.getenv("region"),
                      aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"), 
                      aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
                     aws_session_token = os.getenv("AWS_SESSION_TOKEN"))

text_client = boto3.client("textract", 
                      region_name = os.getenv("region"),
                      aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"), 
                      aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
                     aws_session_token = os.getenv("AWS_SESSION_TOKEN"))

bucket_name = os.getenv('bucket_name')
bucket_path_dest = os.getenv('bucket_path_destination')
traveler_ext = os.getenv('traveler_extension')


def get_priority_list():
    # Purpose to is to create a priority list of documents to OCR
    # this will be any manually selected files first plus whatever hasn't been OCRd yet in S3 minus whatever we have already OCrd


    finished_OCR_dir = os.getenv('OCRd_Output_dir')
    finished_OCR_list = os.listdir(finished_OCR_dir)

    curr_priority_df = pd.read_csv(os.getenv('curr_priority_list'))
    curr_priority_list = list(curr_priority_df['List'])
    


    the_danger = list(set([f for f in curr_priority_list if f.replace('.pdf','') not in finished_OCR_list]))

    return(the_danger)

def wait_out_Progress(jobID):
    retrieve_job = text_client.get_document_analysis(JobId=jobID)
    start = time.time()
    while retrieve_job['JobStatus'] == 'IN_PROGRESS':
        time.sleep(3)
        retrieve_job = text_client.get_document_analysis(JobId=jobID)
    end = time.time()
    print(f'It took Tony Start {end-start} seconds to build this in a cave, with a box of scraps')
    return retrieve_job



def submit_txtrct_job(bucket_name, prefix, file_name):
    key_name = prefix +'/' + file_name

    
    # response = text_client.start_document_text_detection(
    #     DocumentLocation = {'S3Object': {'Bucket': bucket_name, 
    #                                     'Name': key_name}})

    response = text_client.start_document_analysis(
        DocumentLocation = {'S3Object': {'Bucket': bucket_name, 
                                        'Name': key_name}},
        FeatureTypes = ['TABLES'])
    
    # print(response['JobId'])
    
    wait_out_Progress(response['JobId'])
    return response['JobId']
    


def get_blocks(JobId):
    paginationToken = None
    finished = False
    blockslist = []

    while finished == False:

        if paginationToken == None:
            response = text_client.get_document_analysis(JobId =JobId)
        else:
            response = text_client.get_document_analysis(JobId =JobId,
                                                                               NextToken = paginationToken)
        blockslist.append(response['Blocks'])

        if 'NextToken' in response:
            paginationToken = response['NextToken']
        else:
            finished = True

    return blockslist

def blocks_to_csv(traveler_id, blockslist):
    base_dir = os.path.join(os.getenv('OCRd_Output_dir'), traveler_id)
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    i = 0
    for b in blockslist:
        table_csv = get_table_csv_results(b)
        for c in table_csv:
            try:
                df = pd.read_csv(io.StringIO(c), sep = ';', engine='python', header = None)
                df.to_csv(os.path.join(os.getenv('OCRd_Output_dir'), traveler_id, f'{traveler_id} Table {i+1}')+'.csv', index = False)
                i+=1
            except Exception as e:
                print(c)
                print(f'{i+1} had problem: {e}')

            


def main():
    priority_list = get_priority_list()
    prefix = bucket_path_dest + '/' + traveler_ext
    print(len(priority_list))
    for i in tqdm(range(len(priority_list))):
        
        
        f = priority_list[i]

        print(f)
        traveler_id = f.replace('.pdf','')

        JobId = submit_txtrct_job(bucket_name, prefix, f)
        blockslist = get_blocks(JobId)

        blocks_to_csv(traveler_id, blockslist)
        # buh = open('store.pckl', 'wb')
        # pickle.dump(blockslist, buh)
        # buh.close()
        
        # offmain(traveler_id)
        

        


def offmain(traveler_id):
    f = open('store.pckl', 'rb')
    obj = pickle.load(f)
    f.close()
    if not os.path.exists(traveler_id):
        os.makedirs(traveler_id)
    i = 0 
    for b in obj:
        table_csv = get_table_csv_results(b)
        
        df = pd.read_csv(io.StringIO(table_csv), sep = ',')
        df.to_csv(os.path.join(traveler_id, f'{traveler_id} Table {i+1}')+'.csv', index = False)
        

        
                

if __name__ == "__main__":
    main()
    #offmain()