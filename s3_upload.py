import os
import sys
import boto3

BUCKET_NAME = "trackmaven-images"


def upload_images(bucket_name, n_images=None):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    existing_images = [obj.key for obj in bucket.objects.all() if obj.key[-1] != '/']

    local_images = len(os.listdir('tmp/'))
    print("{} local images...".format(local_images))

    if not n_images:
        n_images = local_images

    for i, img in enumerate(os.listdir('tmp/')):
        if img not in existing_images:
            print("{}/{} - Uploading: {}".format((i+1), n_images, img))

            data = open("tmp/{}".format(img), 'rb')
            s3.Bucket(BUCKET_NAME).put_object(Key=img, Body=data)

            os.remove("tmp/{}".format(img))  # delete local copy
        else:
            print("DUPLICATE - deleting {}".format(img))
            os.remove("tmp/{}".format(img))  # remove local files that exist in bucket

        if (i+1) == n_images:
            break
    pass


if __name__ == "__main__":
    try:
        upload_images(BUCKET_NAME, n_images=int(sys.argv[1]))

    except IndexError:
        upload_images(BUCKET_NAME)
