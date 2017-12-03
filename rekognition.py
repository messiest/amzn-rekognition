import boto3
import botocore

from s3_access import S3Bucket


class ObjectDetection:
    """
        object detection using amazon rekognition
    """
    def __init__(self, printer=False):
        self.printer = printer
        self.client = boto3.client('rekognition')
        self.bucket = 'doodle-bot'

    def detect(self, image, threshhold=50):
        labels = []
        response = self.client.detect_labels(Image={'S3Object': {'Bucket': self.bucket,'Name': image}},
                                             MinConfidence=threshhold)
        if self.printer:
            print('Detected labels for ' + image)
            for label in response['Labels']:
                print("  " + label['Name'] + ' : ' + str(label['Confidence']))

        return response['Labels']




if __name__ == "__main__":
    img_check = ObjectDetection()
    bucket = S3Bucket('doodle-bot', printer=True)

    for i, img in enumerate(bucket.keys):
        print(img_check.detect(img, 50))

        if i == 10:
            break