import datetime
import azure.functions as func

import pandas as pd
from azure.storage import CloudStorageAccount
from azure.storage.blob import PublicAccess
import logging
import time
import io

start = time.time()

from datetime import datetime
from datetime import timedelta
import random

import os, requests, uuid, json
from pandas.io.json import json_normalize

# set environment variables
AZURE_STORAGE_ACCOUNT = AZURE_STORAGE_ACCOUNT
AZURE_STORAGE_KEY = AZURE_STORAGE_KEY
AZURE_SENTIMENT_LIMIT = 500
TENANT = TENANT # target folder and file name
EEN_CONTAINER = EEN_CONTAINER  #  download folder
ANALYSIS_CONTAINER = ANALYSIS_CONTAINER  # PowerBI source folder
DOWNLOAD_BLOB_LIMIT = 10000  # Should be max 10000
norm = lambda x: str(x) if x >= 10 else '0' + str(x)

def main(mytimer: func.TimerRequest) -> None:

    try:
        storage_client = CloudStorageAccount(AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY)
        blob_service = storage_client.create_block_blob_service()
    except Exception as e:
        logging.error("Error while creating Azure blob service : {}".format(e))


    def create_storage_container(storage_container):
        """
        creating storage container
        :param storage_container: name of the storage container
        :return:
        """
        try:
            blob_service.create_container(
                storage_container, public_access=PublicAccess.Blob
            )
            logging.info("Container -{}- is created".format(storage_container))
        except Exception as e:
            logging.error("Error while creating storage container : {}".format(e))


    def check_storage_container_exist(storage_container):
        """Checking an Azure storage container is exist in the storage account.
        :param storage_container:
        :return:
        """
        try:
            status = blob_service.exists(container_name=storage_container)

        except Exception as e:
            logging.error("Error while checking storage container {} : {}".format(storage_container, e))
            raise

        return status


    def check_storage_file_exist_old(storage_container, blob_name):
        """checking an Azure storage container is exist in the storage account.
        :param storage_container:
        :return:
        """
        try:
            generator = blob_service.list_blobs(storage_container)
            storage_blob_list = [blob.name for blob in generator]
        except Exception as e:
            logging.error("Error while getting blob list from Azure storage container -{}-".format(storage_container))
            raise

        if blob_name in storage_blob_list:
            logging.info("Storage blob file -{}- exists".format(blob_name))
            status = True
        else:
            logging.info("Storage blob file -{}- does not exist".format(blob_name))
            status = False
        return status


    if check_storage_container_exist(ANALYSIS_CONTAINER) is False:
        create_storage_container(ANALYSIS_CONTAINER)

    now = datetime.now() - timedelta(hours=9)  # default should be set to 4
    LAST_YEAR = str(now.year)
    LAST_MONTH = str(norm(now.month))
    LAST_DAY = str(norm(now.day))  # TODO: CHECK BLOB FILE IF JANUARY IS "01" WE SHOULD FORMAT THIS VARIABLE
    LAST_HOUR = now.strftime("%H")
    LAST_MIN = str(now.minute)
    generator = blob_service.list_blobs(EEN_CONTAINER, delimiter='/',
                                        prefix="{}/{}/{}/{}/".format(LAST_YEAR, LAST_MONTH, LAST_DAY,
                                                                     LAST_HOUR))

    # for counting total number of files downloaded.
    data_counter = []
    for blob in generator:
        data_counter.append(blob.name)
        print(blob.name)

    print("Download started... \nETL Date: {}{} \nNumber of blob files: {}\n.\n.\n.".format(now.strftime("%b %d %Y %H"),
                                                                                            ":00", str(len(data_counter))))

    if len(data_counter) <= DOWNLOAD_BLOB_LIMIT:
        randomlist = data_counter
    else:
        randomlist = random.sample(data_counter, k=DOWNLOAD_BLOB_LIMIT)

    if len(randomlist) > 0:
        print(" analysis is started")
        i = 0
        dataloaded = []
        for blob in randomlist:
            i += 1
            print('file name: {} - number: {}'.format(blob, i))
            loader = blob_service.get_blob_to_text(container_name=EEN_CONTAINER, blob_name=blob)
            trackerstatusobjects = loader.content.split('\n')
            for trackerstatusobject in trackerstatusobjects:
                dataloaded.append(json.loads(trackerstatusobject))

        df = pd.io.json.json_normalize(dataloaded)

        print(df)

        # ---------------------------------WRITE TO CSV------------------------------------------#

        df2 = df[['cameraid', 'event', 'timestamp', 'roiid', 'url']]

        output = io.StringIO()
        output = df2.to_csv(index_label="idx", encoding="utf-8")
        blob_service.create_blob_from_text(container_name=ANALYSIS_CONTAINER,
                                           blob_name='{}\str-{}-key-0001-{}-{}-{}-{}.csv'.format(TENANT, TENANT,
                                                                                                 LAST_YEAR, LAST_MONTH,
                                                                                                 LAST_DAY, LAST_HOUR),
                                           text=output)

elapsed_time = (time.time() - start)
print("Execution Time {} second".format(elapsed_time))
