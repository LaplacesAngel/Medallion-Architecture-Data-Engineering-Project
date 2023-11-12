#none of this worked, made a copy but it was empty, didn't delete the original

import boto3

client = boto3.client('s3')

response = client.list_objects_v2(Bucket='hwe-fall-2023', Prefix='plentz/silver/review/')

source_key = response["Contents"][0]["Key"]

copy_source = {'Bucket': 'hwe-fall-2023', 'Key': source_key}

# Remove the repetition of the source key when specifying the new key
new_key = 'plentz/silver/reviews/'  # This should be the new key

client.copy_object(Bucket='hwe-fall-2023', CopySource=copy_source, Key=new_key)

client.delete_object(Bucket='hwe-fall-2023', Key=source_key)