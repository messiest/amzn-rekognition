import numpy as np
import boto3
import botocore


class S3Bucket:
    """
        a wrapper for connecting to an s3 bucket instance
    """
    def __init__(self, name, printer=False):
        self.name = name
        self.printer = printer
        self.bucket = self.connect()
        self.objects = self.get_objects()
        self.keys = self.get_keys()

    def connect(self):
        s3 = boto3.resource('s3')
        _bucket = s3.Bucket(self.name)
        exists = True
        try:
            s3.meta.client.head_bucket(Bucket=self.name)
        except botocore.exceptions.ClientError as e:
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                print("{} - Bucket does not exist.".format(error_code))
                exists = False

        if self.printer: print("bucket '{}' exists? {}".format(self.name, exists))
        self.bucket = _bucket

        return _bucket

    def get_objects(self):
        self.objects = [object for object in self.bucket.objects.all()]

        return self.objects

    def get_keys(self):
        self.keys = [obj.key for obj in self.objects if obj.key[-1] != '/']

        return self.keys


def main():
    bucket = S3Bucket('doodle-bot')
    bucket.connect()
    bucket.get_objects()
    filenames = bucket.get_keys()

    return filenames

if __name__ == "__main__":
    file_names = [file for file in main()]
    np.random.shuffle(file_names)
    bucket = 'doodle-bot'
    client = boto3.client('rekognition')

    for i, file in enumerate(file_names):
        print(i, file)


        response = client.detect_labels(Image={'S3Object': {'Bucket': bucket, 'Name': file}}, MinConfidence=25)

        print('Detected labels for ' + file)
        for label in response['Labels']:
            print("  " + label['Name'] + ' : ' + str(label['Confidence']))

        if i == 1:
            break
