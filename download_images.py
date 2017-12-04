import os
import sys
import cv2
import urllib
import pandas as pd
import numpy as np

import boto3

from s3_access import S3Bucket

BUCKET_NAME = "trackmaven-images"


def download_image(key, url):
    s3 = boto3.resource('s3')
    if os.path.exists("tmp/{}.jpg".format(key)):  # don't redownload existing files
        return False
    try:
        print("  Downloading: ", url)
        resp = urllib.request.urlopen(url, timeout=3)              # timeout in case of a load error
        image = np.asarray(bytearray(resp.read()), dtype="uint8")  # convert to numpy array
        image = cv2.imdecode(image, cv2.IMREAD_COLOR)              # decode color image
        # TODO (@messiest) add image resizing
        file_path = 'tmp/{}.jpg'.format(key)                       # create file path
        cv2.imwrite(file_path, image)                              # write image to disk
        data = open(file_path, 'rb')
        s3.Bucket('trackmaven-images').put_object(Key="{}.jpg".format(key), Body=data)  # upload to s3

        os.remove(file_path)  


        return True

    except:
        return False


def update_image_bucket(bucket, batch_size=100):
    """
    update local files with images that have not yet been transferred to the s3 bucket

    :param bucket: S3 bucket to access
    :type bucket: str
    :param batch_size: number of images to download
    :type batch_size: int
    :return None
    """
    bucket = S3Bucket(bucket)                              # instantiate bucket
    bucket.connect()                                       # connect to bucket
    images = bucket.get_keys()                             # get list of file names

    data_chunks = pd.read_csv('data/pinterest-blogs.csv',  # import data in chunks (for processing speed)
                              index_col=0,                 # use index column
                              low_memory=False,            # to deal with multiple dtypes
                              chunksize=1000)              # rows per chunk

    downloaded_images = 0                                  # counter for downloads
    for chunk in data_chunks:                              # iterate over all chunks
        for i, j in chunk.iterrows():                      # iterate over rows in chunk
            key = j['uniqueid']                            # get image key
            url = j['image_url']                           # get image url
            file_name = "{}.jpg".format(key)               # generate file name
            file_path = "tmp/{}.jpg".format(key)           # generate file path

            if file_name not in images and not os.path.exists(file_path):  # skip if file exists in bucket or locally
                if url is not np.NaN:  # skip if NaN
                    print("{}/{} - ".format((downloaded_images + 1), batch_size, i), key)
                    image_downloaded = download_image(key, url)
                    if image_downloaded:  # increment if download was successful
                        downloaded_images += 1

            elif file_name in images and os.path.exists(file_path):  # remove files that already exist in the bucket
                print("DUPLICATE - deleting {}".format(file_name))
                os.remove(file_path)

            if downloaded_images == batch_size:  # end if batch is completed
                print("Batch complete.")
                return


def main(bucket_name='trackmaven-images'):
    try:
        update_image_bucket(bucket_name, batch_size=int(sys.argv[1]))
    except IndexError:
        update_image_bucket(bucket_name, batch_size=25)


if __name__ == "__main__":
    main()
