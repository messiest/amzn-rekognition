import sys
import pickle
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

    def detect(self, bucket, image, threshhold=75):
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
        response = self.client.detect_labels(Image={'S3Object': {'Bucket': bucket, 'Name': image}}, MinConfidence=threshhold)
        if self.printer:
            print('Detected labels for ' + image)
            for label in response['Labels']:
                print("  " + label['Name'] + ' : ' + str(label['Confidence']))

        labels = {i['Name']: i['Confidence'] for i in response['Labels']}  # dictionary of features:confidences

        return labels


def main(n=10):
    obj_detect = ObjectDetection()
    bucket = S3Bucket('trackmaven-images')
    bucket.connect()

    try:
        results = pickle.load(open('image-tags.pkl', 'rb'))
        print('loaded pickle')
        print("Total Images: {}".format(len(results.keys())))

    except:
        results = {}

    processed_images = 0
    while processed_images < n:
        for i, img in enumerate(bucket.sample(n)):
            try:
                if img not in results.keys():  # skip files that have already been processed
                    labels = obj_detect.detect('trackmaven-images', img)
                    results[img] = labels  # dict[image] = {features:confidence})
                    processed_images += 1

                    print("{}/{} - ".format(processed_images, n), img, labels)
                else:
                    continue

            except botocore.errorfactory.InvalidImageFormatException:  # avoids breaking on empty images
                continue
        break

    pickle.dump(results, open('image-tags.pkl', 'wb'))

    return results


if __name__ == "__main__":
    try:
        main(n=int(sys.argv[1]))
    except IndexError:
        main()
