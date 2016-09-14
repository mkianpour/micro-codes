# -*- encoding: utf-8 -*-
import sys
import boto3
import botocore
import datetime
import difflib

AWS_REGION = 'YOUR_S3_AWS_REGION'

def lambda_handler(event, context):
	firstRun = False
	logFileData = ""

	S3BucketName = 'NAME_OF_BUCKET'
	S3BucketPrefix = 'DIRECTORY_INSIDE_BUCKET'
	RDSInstanceNames = ['LIST_OF_RDS_INSTANCES']
	logNamePrefix = 'PREFIX_OF_LOGFILE_NAME' #defined in parameters group of RDS instance
	lastRecievedFile = S3BucketPrefix + 'lastMarker'
	region = AWS_REGION
	mailto=['RECEPIENTS_EMAILS_AS_ARRAY']
	sender='SENDER_EMAIL_ADDRESS'
	distinguisher='duration'

	RDSclient = boto3.client('rds',region_name=region)
	S3client = boto3.client('s3')
	SESclient = boto3.client('ses')

	today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
	logNamePrefix = logNamePrefix + today
	for RDSInstanceName in RDSInstanceNames:
		dbLogs = RDSclient.describe_db_log_files( DBInstanceIdentifier=RDSInstanceName, FilenameContains=logNamePrefix)
		lastWrittenTime = 0
		lastWrittenThisRun = 0
		try:
			S3response = S3client.head_bucket(Bucket=S3BucketName)
		except botocore.exceptions.ClientError as e:
			error_code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
			if error_code == 404:
				return "Error: Bucket name provided not found"
			else:
				return "Error: Unable to access bucket name, error: " + e.response['Error']['Message']
		try:
			S3response = S3client.get_object(Bucket=S3BucketName, Key=lastRecievedFile + RDSInstanceName)
		except botocore.exceptions.ClientError as e:
			error_code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
			if error_code == 404:
				print("It appears this is the first log import, all files will be retrieved from RDS")
				firstRun = True
			else:
				return "Error: Unable to access lastRecievedFile name, error: " + e.response['Error']['Message']

		if firstRun == False:
			lastWrittenTime = int(S3response['Body'].read(S3response['ContentLength']))
			print("Found marker from last log download, retrieving log files with lastWritten time after %s" % str(lastWrittenTime))
		for dbLog in dbLogs['DescribeDBLogFiles']:
			if ( dbLog['LogFileName'] == logNamePrefix and int(dbLog['LastWritten']) > lastWrittenTime ) or firstRun:
				print("Downloading log file: %s found and with LastWritten value of: %s " % (dbLog['LogFileName'],dbLog['LastWritten']))
				if int(dbLog['LastWritten']) > lastWrittenThisRun:
					lastWrittenThisRun = int(dbLog['LastWritten'])
				logFile = RDSclient.download_db_log_file_portion(DBInstanceIdentifier=RDSInstanceName, LogFileName=dbLog['LogFileName'],Marker='0')
				logFileData = logFile['LogFileData']
				objectName = S3BucketPrefix + dbLog['LogFileName'] + RDSInstanceName
				try:
					S3response = S3client.get_object(Bucket=S3BucketName, Key=objectName)
				except botocore.exceptions.ClientError as e:
					print("%s", e.response['Error']['Message'])
				oldData = S3response['Body'].read()
				oldData = oldData.splitlines(1)
				newData = logFileData.splitlines(1)
				diff = difflib.unified_diff(oldData, newData)
				lines = list(diff)
				added = [line[1:] for line in lines if line[0] == '+']
				added = ''.join(added)
				for msg in added.split(today):
					print("message: %s", msg)
					if distinguisher in msg:
						SESclient.send_email(
							Source=sender,
							Destination={'ToAddresses': mailto},
							Message={
								'Subject': {
								'Data': 'RDS Slow Query Log',
								'Charset': 'UTF-8'
								},
								'Body': {
									'Text': {
										'Data': msg,
										'Charset': 'UTF-8'
									}
								}
							}
						)
						print("Slow Query found and email sent %s", msg)
				logFileData += logFile['LogFileData']
				byteData = str.encode(logFileData)
				try:
					objectName = S3BucketPrefix + dbLog['LogFileName'] + RDSInstanceName
					S3response = S3client.put_object(Bucket=S3BucketName, Key=objectName,Body=byteData)
				except botocore.exceptions.ClientError as e:
					return "Error writting object to S3 bucket or SES, ClientError: " + e.response['Error']['Message']
				print("Writting log file %s to S3 bucket %s" % (objectName,S3BucketName))
		try:
			S3response = S3client.put_object(Bucket=S3BucketName, Key=lastRecievedFile + RDSInstanceName, Body=str.encode(str(lastWrittenThisRun)))
		except botocore.exceptions.ClientError as e:
			return "Error writting object to S3 bucket, S3 ClientError: " + e.response['Error']['Message']
	print("Wrote new Last Written Marker to %s in Bucket %s" % (lastRecievedFile ,S3BucketName))
	return "Log file export complete"
