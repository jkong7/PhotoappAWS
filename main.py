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

    #Retrive number of users in users table
    sql_users_info = """
    SELECT COUNT(*) 
    FROM users; 
    """

    row_users = datatier.retrieve_one_row(dbConn, sql_users_info)
    if row_users is None: 
      print("Failed to retrieve any user rows")
    else: 
      print("# of users: ", row_users[0]) 
    

    #Retrive number of assets in assets table
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

    #Retrive all columns and in descending order by userid 
    sql_users_output = """
    SELECT * 
    FROM users 
    ORDER BY userid DESC; 
    """

    #We want all the rows 
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
    #Retrive all columns and in descending order by assetid
    sql_asset_output = """
    SELECT * 
    FROM assets 
    ORDER BY assetid DESC; 
    """

    #We want all the rows 
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
def download(dbConn, bucket, display=False): 
    """
    Retrieves asset file by assetid in the asset table, downloads file, 
    and then renames it based on original name. Shows image if user inputs
    5 (display boolean will be passed as True)
  
    Parameters
    ----------
    dbConn: open connection to MySQL server
    display: boolean-controls whether to show downloaded image
  
    Returns
    -------
    nothing
    """
    try: 
        print("Enter asset id>")
        cmd = input()

        #Query the database for assetname and bucketkey based on assetid
        sql_download_input_name = """
        SELECT assetname, bucketkey 
        FROM assets 
        WHERE assetid = %s; 
        """

        row = datatier.retrieve_one_row(dbConn, sql_download_input_name, [cmd]) 

        if row is None or row==(): 
            print("No such asset...")
            return

        #Retrieve the assetname (original name) and bucketkey (S3 key)
        assetname = row[0]   
        bucketkey = row[1]

        # Use the helper function to download the file
        downloaded_filename = awsutil.download_file(bucket, bucketkey)

        if downloaded_filename is None:
            print(f"Error: Failed to download file from S3 with key {bucketkey}")
        else:
            #Rename the downloaded file to the original asset name
            os.rename(downloaded_filename, assetname)
            print(f"Downloaded from S3 and saved as ' {assetname} '")

            # If display=True, trigger image to pop up
            if display: 
                image = img.imread(assetname)
                plt.imshow(image)
                plt.show()

    except Exception as e:
        print("ERROR")
        print("ERROR: an exception was raised and caught")
        print("MESSAGE:", str(e))


###################################################################
#
# upload
#
def upload(dbConn, bucket): 
    """
    Inputs a local file, a user id, and uploads this file to the user's folder
    in S3 (file is given a unique uuid name). Also inputs all asset information into the 
    asset table as a row 
  
    Parameters
    ----------
    dbConn: open connection to MySQL server
  
    Returns
    -------
    nothing
    """
    try: 
        # Local filename + error handling 
        print("Enter local filename>")
        cmd_filename = input()

        if not os.path.isfile(cmd_filename): 
            print(f"Local file '{cmd_filename}' does not exist...")
            return  

        print("Enter user id>")
        cmd_userid = input()

        # Check if this query does not come up empty, which indicates the user exists
        sql_check_user = """
        SELECT bucketfolder FROM users
        WHERE userid = %s;
        """

        row = datatier.retrieve_one_row(dbConn, sql_check_user, [cmd_userid])

        if row is None: 
            print("No such user...")
            return 

        #Construct uuid-based bucket key name
        folder_id = row[0]
        file_id = str(uuid.uuid4())
        bucket_key = f"{folder_id}/{file_id}.jpg"

        uploaded_key = awsutil.upload_file(cmd_filename, bucket, bucket_key)

        if uploaded_key is None:
            print(f"Error uploading file to S3 as '{bucket_key}'")
            return 
        else:
            print(f"Uploaded and stored in S3 as '{uploaded_key}'")

        #Insert row containing new asset info into the assets table
        sql_insert_asset = """
        INSERT INTO assets (userid, assetname, bucketkey)
        VALUES (%s, %s, %s)
        """

        rows = datatier.perform_action(dbConn, sql_insert_asset, [cmd_userid, cmd_filename, uploaded_key])

        if rows == -1: 
            print("Error inserting asset into assets table")
            return
        
        #Output the auto-generated asset id from the inputted row
        sql_last_insert = """
        SELECT LAST_INSERT_ID()
        """

        row = datatier.retrieve_one_row(dbConn, sql_last_insert)
        last_asset_id = row[0]
        print(f"Recorded in RDS under asset id {last_asset_id}")

    except Exception as e:
        print("ERROR")
        print("ERROR: an exception was raised and caught")
        print("MESSAGE:", str(e))


###################################################################
#
# add_user 
#
def add_user(dbConn): 
    """
    Retrieves asset file by assetid in the asset table, downloads file, 
    and then renames it based on original name. Shows image if user inputted
    5 (display boolean will be passed as True)
  
    Parameters
    ----------
    dbConn: open connection to MySQL server
  
    Returns
    -------
    nothing
    """
    try: 
      #Retrive email, lastname, and firstname from input 
      print("Enter user's email>")
      cmd_user_email = input()

      print("Enter user's last (family) name>")
      cmd_last_name = input()

      print("Enter user's first (family) name>")
      cmd_first_name = input()

      #Create uuid folder_name
      folder_name=str(uuid.uuid4())

      #Action query-insert new user into users table
      sql_insert_user = """
      INSERT INTO users (email, lastname, firstname, bucketfolder)
      VALUES (%s, %s, %s, %s)
      """

      rows = datatier.perform_action(dbConn, sql_insert_user, [cmd_user_email, cmd_last_name, cmd_first_name, folder_name])

      if rows == -1: 
        print("Error inserting user into users table")
      
      #Lastly, output the auto-generated user id from the inputted row
      sql_last_insert = """
      SELECT LAST_INSERT_ID()
      """

      row = datatier.retrieve_one_row(dbConn, sql_last_insert)
      last_user_id = row[0]
      print(f"Recorded in RDS under {last_user_id}")
  

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
  if cmd == 1:
    stats(bucketname, bucket, endpoint, dbConn)
  elif cmd == 2: 
    users(dbConn)
  elif cmd == 3: 
    assets(dbConn)
  elif cmd == 4: 
    download(dbConn, bucket)
  elif cmd == 5: 
    download(dbConn, bucket, True)
  elif cmd == 6: 
     upload(dbConn, bucket) 
  elif cmd == 7: 
    add_user(dbConn)
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
