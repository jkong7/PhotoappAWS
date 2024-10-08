#
# Main program for photoapp program using AWS S3 and RDS to
# implement a simple photo application for photo storage and
# viewing.
#
# Authors:
#   Jonathan Kong 
#   Prof. Joe Hummel (initial template)
#   Northwestern University
#

import datatier  # MySQL database access
import awsutil  # helper functions for AWS
import boto3  # Amazon AWS

import uuid
import pathlib
import logging
import sys
import os

from configparser import ConfigParser

import matplotlib.pyplot as plt
import matplotlib.image as img


###################################################################
#
# prompt
#
def prompt():
  """
  Prompts the user and returns the command number
  
  Parameters
  ----------
  None
  
  Returns
  -------
  Command number entered by user (0, 1, 2, ...)
  """

  try:
    print()
    print(">> Enter a command:")
    print("   0 => end")
    print("   1 => stats")
    print("   2 => users")
    print("   3 => assets")
    print("   4 => download")
    print("   5 => download and display")
    print("   6 => upload")
    print("   7 => add user")

    cmd = int(input())
    return cmd

  except Exception as e:
    print("ERROR")
    print("ERROR: invalid input")
    print("ERROR")
    return -1

###################################################################
#
# stats
#
def stats(bucketname, bucket, endpoint, dbConn):
  """
  Prints out S3 and RDS info: bucket name, # of assets, RDS 
  endpoint, and # of users and assets in the database
  
  Parameters
  ----------
  bucketname: S3 bucket name,
  bucket: S3 boto bucket object,
  endpoint: RDS machine name,
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  #
  # bucket info:
  #
  try: 
    print("S3 bucket name:", bucketname)

    assets = bucket.objects.all()
    print("S3 assets:", len(list(assets)))

    #
    # MySQL info:
    #
    print("RDS MySQL endpoint:", endpoint)

    sql_users_info = """
    SELECT COUNT(*) 
    FROM users; 
    """

    row_users = datatier.retrieve_one_row(dbConn, sql_users_info)
    if row_users is None: 
      print("Failed to retrieve any user rows")
    else: 
      print("# of users: ", row_users[0]) 
    
    sql_assets_info = """
    SELECT COUNT(*) 
    FROM assets; 
    """

    row_assets = datatier.retrieve_one_row(dbConn, sql_assets_info)
    if row_assets is None: 
      print("Failed to retrieve any user rows")
    else: 
      print("# of assets: ", row_assets[0]) 

  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))


###################################################################
#
# users
#
def users(dbConn): 
  """
  Retrieves and outputs user information from the user table: userid, 
  email, name, and folder 

  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """

  try: 
    sql_users_output = """
    SELECT * 
    FROM users 
    ORDER BY userid DESC; 
    """
    rows = datatier.retrieve_all_rows(dbConn, sql_users_output)
    if rows is None: 
      print("Failed to retrieve any user rows")
    else: 
      for row in rows: 
        print("User id:", row[0], "\n  Email:", row[1], "\n  Name:", row[2]+" , "+row[3], "\n  Folder:", row[4])
  except Exception as e: 
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))

###################################################################
#
# assets
#
def assets(dbConn): 
  """
  Retrieves and outputs asset information from the asset table: assetid, 
  userid, original name, and key name 

  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """

  try: 
    sql_asset_output = """
    SELECT * 
    FROM assets 
    ORDER BY assetid DESC; 
    """

    rows = datatier.retrieve_all_rows(dbConn, sql_asset_output)
    if rows is None: 
      print("Failed to retrieve any asset rows")
    else: 
      for row in rows: 
        print("Asset id:", row[0], "\n  User id:", row[1], "\n  Original name:", row[2], "\n  Key name:", row[3])
  except Exception as e: 
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))
    
###################################################################
#
# download
#
def download(dbConn): 
    """
    Retrieves asset file by assetid in the asset table, downloads file, 
    and then renames it based on original name.
  
    Parameters
    ----------
    dbConn: open connection to MySQL server
  
    Returns
    -------
    nothing
    """
    try: 
        print("Enter asset id>")
        cmd = input()

        # Query the database for assetname and bucketkey based on assetid
        sql_download_input_name = """
        SELECT assetname, bucketkey 
        FROM assets 
        WHERE assetid = %s; 
        """

        # User input as part of the query dynamically 
        cursor = dbConn.cursor()
        cursor.execute(sql_download_input_name, (cmd,))
        row = cursor.fetchone() 
        cursor.close() 

        if row is None: 
            print("No such asset...") 
            return

        # Retrieve the assetname (original name) and bucketkey (S3 key)
        assetname = row[0]   
        bucketkey = row[1]   

        s3 = boto3.client('s3')
        bucket_name = 'photoapp-jonnykong-310' 

        # Error handling for checking if the object exists in S3
        try:
            s3.head_object(Bucket=bucket_name, Key=bucketkey)
            print(f"{assetname} exists, downloading...")

            # Download the file from S3 and save it locally as the original asset name
            s3.download_file(bucket_name, bucketkey, assetname)
            print(f"Downloaded from S3 and saved as '{assetname}'")
            
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                print("404 Error: File not found in S3.")
            else: 
                print(f"Error: {e}")

    except Exception as e:
        print("ERROR")
        print("ERROR: an exception was raised and caught")
        print("MESSAGE:", str(e))





#########################################################################
# main
#
print('** Welcome to PhotoApp **')
print()

# eliminate traceback so we just get error message:
sys.tracebacklimit = 0

#
# what config file should we use for this session?
#
config_file = 'photoapp-config.ini'

print("What config file to use for this session?")
print("Press ENTER to use default (photoapp-config.ini),")
print("otherwise enter name of config file>")
s = input()

if s == "":  # use default
  pass  # already set
else:
  config_file = s

#
# does config file exist?
#
if not pathlib.Path(config_file).is_file():
  print("**ERROR: config file '", config_file, "' does not exist, exiting")
  sys.exit(0)

#
# gain access to our S3 bucket:
#
s3_profile = 's3readwrite'

os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file

boto3.setup_default_session(profile_name=s3_profile)

configur = ConfigParser()
configur.read(config_file)
bucketname = configur.get('s3', 'bucket_name')

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucketname)

#
# now let's connect to our RDS MySQL server:
#
endpoint = configur.get('rds', 'endpoint')
portnum = int(configur.get('rds', 'port_number'))
username = configur.get('rds', 'user_name')
pwd = configur.get('rds', 'user_pwd')
dbname = configur.get('rds', 'db_name')

dbConn = datatier.get_dbConn(endpoint, portnum, username, pwd, dbname)

if dbConn is None:
  print('**ERROR: unable to connect to database, exiting')
  sys.exit(0)

#
# main processing loop:
#
cmd = prompt()

while cmd != 0:
  #
  if cmd == 1:
    stats(bucketname, bucket, endpoint, dbConn)
  elif cmd == 2: 
    users(dbConn)
  elif cmd == 3: 
    assets(dbConn)
  elif cmd == 4: 
    download(dbConn)
  #
  #
  # TODO
  #
  #
  else:
    print("** Unknown command, try again...")
  #
  cmd = prompt()

#
# done
#
print()
print('** done **')
