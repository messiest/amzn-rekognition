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
        self.bucket = 'trackmaven-images'

    def detect(self, bucket, image, threshhold=100):
        labels = []
        while len(labels) == 0:
            response = self.client.detect_labels(Image={'S3Object': {'Bucket': bucket, 'Name': image}},
                                                 MinConfidence=threshhold)
            if self.printer:
                print('Detected labels for ' + image)
                for label in response['Labels']:
                    print("  " + label['Name'] + ' : ' + str(label['Confidence']))

            threshhold -= 5
            if threshhold <= 5:  # break if no images accessed
                break

            labels = response['Labels']

        return labels

def main():
    img_check = ObjectDetection()

    bucket = S3Bucket('trackmaven-images')
    bucket.connect()

    for i, img in enumerate(bucket.sample(10)):

        try:
            print(img, img_check.detect('trackmaven-images', img))

        except botocore.errorfactory.InvalidImageFormatException:  # avoids empty images
            continue


if __name__ == "__main__":
    main()
