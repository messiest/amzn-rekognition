import numpy as np
import boto3
import botocore


class S3Bucket:
    """
        wrapper for connecting to an s3 bucket instance
    """
    def __init__(self, name, printer=False):
        self.name = name
        self.printer = printer
        self.bucket = self.connect()
        self.objects = None
        self.keys = None

    def connect(self):
        """
            connect to s3 instance
        """
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

        if self.printer:
            print("bucket '{}' exists? {}".format(self.name, exists))

        self.bucket = _bucket
        self.objects = self.get_objects()
        self.keys = self.get_keys()

    def get_objects(self):
        """
        get list of objects in s3 bucket

        :return: a list of s3 objects in the bucket
        :rtype: list
        """
        self.objects = [object for object in self.bucket.objects.all()]

        return self.objects

    def get_keys(self):
        """
        get list of file names in s3 bucket

        :return: self.keys
        :rtype: list
        """
        self.keys = [obj.key for obj in self.objects if obj.key[-1] != '/']

        return self.keys

    def sample(self, n):
        """
        return random sample of n objects from s3 bucket
        
        :param n: number of items
        :type n: int
        :return: sample
        :rtype: list
        """

        try:
            sample = list(np.random.choice(self.keys, n))

        except ValueError:
            sample = []

        return sample


def main():
    bucket = S3Bucket('doodle-bot')
    bucket.connect()
    bucket.get_objects()
    filenames = bucket.get_keys()
    print(len(filenames))


if __name__ == "__main__":
    main()
