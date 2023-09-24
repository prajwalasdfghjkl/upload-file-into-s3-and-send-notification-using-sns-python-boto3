#Upload-File-into-S3-and-Send-Notification-Using-SNS-Python-boto3
import pandas as pd
import boto3
from io import StringIO

data = [['tom', 10], ['nick', 15], ['juli', 14], ['bob', 20], ['alice', 30]]

# Create the pandas DataFrame
df = pd.DataFrame(data, columns=['Name', 'Age'])

# print dataframe.
print(df)

ACCESS_KEY= "AKIARHLK4GZA3FLKYVO5"
SECRET_ACCESS_KEY= "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"

#Creating a boto3 client for accessing access key and secret key
def upload_to_s3(df):
    f = 'file.csv'
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key =SECRET_ACCESS_KEY)

    #Converting a dataframe(df) to csv file 
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)

    #uploading file to s3
    s3.put_object(Bucket='prajwal20', Body=csv_buf.getvalue(), Key='folder/' +f)

#Creating SNS client
ses = boto3.client('sns', aws_access_key_id =ACCESS_KEY, aws_secret_access_key =SECRET_ACCESS_KEY, region_name='ap-south-1')
sns_topicname_arn = "arn:aws:sns:ap-south-1:084512880193:alarmp"

#Publish message to SNS topic
def publishMessage(snsArn, msg):
    client = boto3.client('sns', aws_access_key_id =ACCESS_KEY, aws_secret_access_key =SECRET_ACCESS_KEY, region_name='ap-south-1')
    client.publish(TargetArn=snsArn, Message=msg)

upload_to_s3(df)

msg = "The file(file,csv) has been uploaded into your s3 bucket"

publishMessage(sns_topicname_arn, msg)