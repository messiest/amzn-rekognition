import sys
import boto3
import botocore

from s3_access import S3Bucket


class ObjectDetection:  # TODO (@messiest) change this to a function?
    """
        object detection using amazon rekognition
    """
    def __init__(self, printer=False):
        self.printer = printer
        self.client = boto3.client('rekognition')

    def detect(self, bucket, image, threshhold=100):
        """
        run object detection on image

        :param bucket: name of s3 bucket
        :type bucket: str
        :param image: file name of image
        :type image: str
        :param threshhold: starting confidence threshold
        :type threshhold: int
        :return: dictionary of labels
        :rtype:
        """
        labels = []
        while len(labels) == 0:
            response = self.client.detect_labels(Image={'S3Object': {'Bucket': bucket, 'Name': image}},
                                                 MinConfidence=threshhold)
            if self.printer:
                print('Detected labels for ' + image)
                for label in response['Labels']:
                    print("  " + label['Name'] + ' : ' + str(label['Confidence']))

            threshhold -= 5
            if threshhold <= 5:  # break if no objects detected
                break

            labels = {i['Name']: i['Confidence'] for i in response['Labels']}  # dictionary of features:confidences

        return labels


def main(n=10):
    obj_detect = ObjectDetection()
    bucket = S3Bucket('trackmaven-images')
    bucket.connect()
    results = []
    for i, img in enumerate(bucket.sample(n)):
        try:
            labels = obj_detect.detect('trackmaven-images', img)
            print("{}/{} - ".format((i+1), n), img, labels)
            results.append((img, labels))  # named tuple of (image, {features:confidence})

        except botocore.errorfactory.InvalidImageFormatException:  # avoids breaking on empty images
            continue

    return results


if __name__ == "__main__":
    try:
        main(n=int(sys.argv[1]))
    except IndexError:
        main()
