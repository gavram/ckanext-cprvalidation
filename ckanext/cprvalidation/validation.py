# encoding: utf-8
import sys
import re
import os

import logging
import psycopg2
import urllib2
import datetime
from ckan.logic import get_action
from ckan.lib.cli import CkanCommand
from ckan.common import config
from time import sleep
from pprint import pprint

log = logging.getLogger(__name__)

class Validation(CkanCommand):
    '''Performs CPR Validation.

    Usage:
        validation initdb
            Creates the database, must have configured the config with the correct password

        validation scan
            Scans ckan for new resources and changes and validates them, run this periodically
    '''

    summary = __doc__.split('\n')[0]
    usage = __doc__

    def command(self):
        self._load_config()

        if len(self.args) == 0:
            self.parser.print_usage()
            sys.exit(1)
        cmd = self.args[0]

        if cmd == 'initdb':
            self.initdb()
        elif cmd == 'scan':
            self.scan()
        elif cmd == 'report':
            self.report()
        elif cmd == 'addexception':
            if(len(self.args) < 2):
                print("required --id option missing")
                sys.exit(1)
            elif(len(self.args) == 2):
                self.addexception(self.args[1])
        else:
            print('Command %s not recognized' % cmd)
            sys.exit(1)

    def addexception(self, id):
        # Adds an exception to the database.
        # Sometimes resources will contain valid CPR-numbers which are in fact not

        d_port = config.get('ckan.cprvalidation.postgres_port', None)
        d_pass = config.get('ckan.cprvalidation.cprvalidation_password', None)

        add_exception = ''' UPDATE cprvalidation.status SET
                    excepted = TRUE
                    WHERE resource_id = %s
                    returning *
        ;'''
        if d_pass == None:
            print("Setup cprvalidation_password in /etc/ckan/default/production.ini")
        if d_port == None:
            print("Setup postgres_port in /etc/ckan/default/production.ini")

        try:
            conn = psycopg2.connect(database="cprvalidation", host="localhost", user="cprvalidation", password=d_pass,
                                    port=d_port)
            conn.autocommit = True
            print(" ")
        except Exception as e:
            print(e)
            sys.exit()

        cur = conn.cursor()
        cur.execute(add_exception, (id,))

        count = len(cur.fetchall())
        if(count == 0):
            print("Could not find relation %s " % id)
        else:
            print("Added exception for %s " % id)
        conn.commit()
        conn.close()

    def initdb(self):
        #For debugging purposes we delete the database everytime we init. This CLEANS the database
        #create_user = '''
        #    DROP OWNED BY cprvalidation;
        #    DROP ROLE IF EXISTS cprvalidation;
        #    CREATE ROLE cprvalidation WITH PASSWORD %s;
        #    END;
        #'''

        d_port = config.get('ckan.cprvalidation.postgres_port', None)
        d_pass = config.get('ckan.cprvalidation.cprvalidation_password', None)
        postgres_pass = config.get('ckan.cprvalidation.postgres_password', None)

        if d_pass == None:
            print("Setup cprvalidation_password in /etc/ckan/default/production.ini")
        if d_port == None:
            print("Setup postgres_port in /etc/ckan/default/production.ini")
        if postgres_pass == None:
            print("Setup postgres_password in /etc/ckan/default/production.ini")

        drop_db = '''DROP DATABASE IF EXISTS cprvalidation;'''
        create_db = '''

            CREATE DATABASE cprvalidation
            WITH OWNER = cprvalidation
            ENCODING = 'UTF8'
            TABLESPACE = pg_default
            CONNECTION LIMIT = -1;
        '''

        create_schema = '''
            DROP SCHEMA IF EXISTS cprvalidation ;
            CREATE SCHEMA cprvalidation
            AUTHORIZATION cprvalidation;
        '''

        create_table = '''
            DROP TABLE IF EXISTS cprvalidation.status;
            CREATE TABLE cprvalidation.status
            (
              package_id character varying NOT NULL,
              resource_id character varying NOT NULL,
              status character varying, -- valid, invalid, pending
              format character varying NOT NULL,
              url character varying,
              url_type character varying,
              datastore_active character varying,
              last_checked character varying,
              last_updated character varying,
              cpr_number character varying,
              excepted boolean,
              CONSTRAINT status_pkey PRIMARY KEY (resource_id)
            )
            WITH (
              OIDS=FALSE
            );
            ALTER TABLE cprvalidation.status
              OWNER TO cprvalidation;
            COMMENT ON COLUMN cprvalidation.status.status IS 'valid, invalid, pending';

        '''

        try:
            conn = psycopg2.connect(database="postgres", host="localhost", user="postgres", password=postgres_pass,
                                    port=d_port)
            conn.autocommit = True
            print("Connected as postgres user.")
        except Exception as e:
            print(e)
            sys.exit()

        cur = conn.cursor()
        #cur.execute(create_user,[d_password])
        cur.execute(drop_db)
        cur.execute(create_db)
        print("Initialized Database")
        conn.commit()
        conn.close()
        try:
            conn = psycopg2.connect(database="cprvalidation", host="localhost", user="cprvalidation",
                                        password=d_pass,
                                        port=d_port)
            conn.autocommit = True

            print("Created the table and scheme")
        except Exception as e:
            print("Woops")
            print(e)
            sys.exit()

        cur = conn.cursor()
        cur.execute(create_schema)
        cur.execute(create_table)
        print("Created schema and table")
        conn.commit()
        conn.close()
        print("Done.")

    def scan(self):
        resource_list = getAllResources()  # list of all resources in CKAN
        print("%d resources in catalog \n" % len(resource_list))
        # Update the database with new resources and / or packages
        updateSchema(resource_list)
        resources_to_check = scanDB()
        count = 0

        if(len(resources_to_check) == 0):
            print("No updates, no resources to scan")
            return

        for r in resources_to_check:
            validateResource(r)
            count+= 1

        print("Scanned %d resources for CPR numbers" % count)

# # # #
# Helper Functions
# # # #
def processCSV(file_path,local):
    # We'll use the package_create function to create a new dataset.
    request = urllib2.Request(
        'http://www.my_ckan_site.com/api/action/package_create')

    # Creating a dataset requires an authorization header.
    # Replace *** with your API key, from your user account on the CKAN site
    # that you're creating the dataset on.
    request.add_header('Authorization', '***')

    retrycount = 0
    #TODO: This is probably not the best way to handle a CSV file..
    if(local):
        with open(file_path) as f:
            string = f.read().replace(',', ' ')
            return string
    else:
        try:
            # We'll use the package_create function to create a new dataset.
            request = urllib2.Request(file_path)

            #TODO: Add API from config, good if you want a dedicated CKAN user to scan
            request.add_header("Authorization", "0114f011-606b-46d1-b96e-01a1ae287a2d")
            response = urllib2.urlopen(request)
            string = response.read().replace(',',' ')
            return string
        except urllib2.HTTPError as e:
            if e.code == 404:
                print("404 file was not found")
                #Don't try again as the file was not found
                return None
            elif e.code == 500:
                print("500 Internal Server Error.. reconnecting")
                if(retrycount < 5):
                    for i in range(5):
                        try:
                            # We'll use the package_create function to create a new dataset.
                            request = urllib2.Request(file_path)

                            # TODO: Add API from config, good if you want a dedicated CKAN user to scan
                            request.add_header("Authorization", "0114f011-606b-46d1-b96e-01a1ae287a2d")
                            response = urllib2.urlopen(request)
                            string = response.read().replace(',', ' ')
                            return string
                        except urllib2.HTTPError as e:
                            retrycount += 1
                            print("Retrying...")
                        sleep(5)
            elif e.code == 504:
                print("Gateway Timed Out, is file too big?")
                return None
            else:
                print("error: " + str(e.code))
                return None


def validateResource(resource):
    '''       Overview of the tuple
         (
                 0 package_id character varying NOT NULL,
                 1 resource_id character varying NOT NULL,
                 2 status character varying, -- valid, invalid, pending
                 3 format character varying NOT NULL,
                 4 url character varying,
                 5 url_type character varying,
                 6 datastore_active character varying,
                 7 last_checked character varying,
                 8 last_updated character varying,
                )
        '''
    siteurl = config.get('ckan.site_url')
    validformats = ["csv"]
    d_port = config.get('ckan.cprvalidation.postgres_port', None)
    d_pass = config.get('ckan.cprvalidation.cprvalidation_password', None)

    id = resource[1]
    format = str(resource[3]).lower()
    datastore = True if str(resource[6]).lower() == "true" else False
    filestore = True if resource[5] == "upload" else False


    file_string = None
    file_path = None
    local = False
    error = False

    print("DEBUG INFO: ")
    print("Datastore: " +str(datastore))
    print("Filestore: " + str(filestore))
    print("Format: " + str(format))

    # Get the filepath, locally or externally, it should not matter
    if filestore:
        file_path = os.path.join("/var/lib/ckan/default/resources/",
                                 id[0:3], id[3:6], id[6:])
        local = True
    elif datastore:
        file_path = siteurl + "/datastore/dump/" + id + "?format=csv"

    print("File_path: " + str(file_path))

    #Null checking:
    if format not in validformats:
        print("Format %s can't be processed" % format)
        return None

    if file_path is None:
        print("Could not construct file_path")
        return None

    #Converts the file to a string we can regex and check
    if format == "csv":
        file_string = processCSV(file_path,local)

        #we got the file_string
    if(file_string is None):
        #TODO: Implement an error state
        error = True
    else:
        iscpr = validcpr(file_string)


    if(error):
        try:
            conn = psycopg2.connect(database="cprvalidation", host="localhost", user="cprvalidation", password=d_pass,
                                    port=d_port)
        except Exception as e:
            print(e)
            sys.exit()
        current_time = datetime.datetime.now()  # Timestamp
        insert = """
                    UPDATE cprvalidation.status
                    SET status='error', last_checked= %s
                    WHERE resource_id= %s
                    returning *
                ;"""

        cur = conn.cursor()
        cur.execute(insert, [current_time, id])
        conn.commit()
        conn.close()
        return
    if(file_string != None):

        if(not iscpr[0]): #If we dont have a CPR in the resource
            try:
                conn = psycopg2.connect(database="cprvalidation",host="localhost", user="cprvalidation",password=d_pass,port=d_port)
            except Exception as e:
                print(e)
                sys.exit()
            current_time = datetime.datetime.now() #Timestamp
            insert = """
                        UPDATE cprvalidation.status
                        SET status='valid', last_checked= %s
                        WHERE resource_id= %s
                        returning *
                    ;"""

            cur = conn.cursor()
            cur.execute(insert, [current_time,id])
            conn.commit()
            conn.close()
        else: #We have a CPR-number!
            try:
                conn = psycopg2.connect(database="cprvalidation",host="localhost", user="cprvalidation",password=d_pass,port=d_port)
            except Exception as e:
                print(e)
                sys.exit()
            current_time = datetime.datetime.now() #Timestamp
            insert = """
                        UPDATE cprvalidation.status
                        SET status='invalid', last_checked= %s,cpr_number=%s
                        WHERE resource_id= %s
                        returning *
            ;"""

            cur = conn.cursor()
            cur.execute(insert, [current_time,iscpr[1],id])
            conn.commit()
            conn.close()

def scanDB():
    d_port = config.get('ckan.cprvalidation.postgres_port', None)
    d_pass = config.get('ckan.cprvalidation.cprvalidation_password', None)

    try:
        conn = psycopg2.connect(database="cprvalidation", host="localhost", user="cprvalidation", password=d_pass,
                                port=d_port)
    except Exception as e:
        print(e)
        sys.exit()

    # TODO: this query needs to pick all the formats
    select = """
                   SELECT * FROM cprvalidation.status
                   WHERE format = 'CSV'
                   AND (last_updated::DATE >= last_checked::DATE OR last_checked IS NULL)
                   AND (url_type IS NOT NULL OR datastore_active = 'true');
       """
    print("Scanning for updates...")
    cur = conn.cursor()
    # Get the datasets we have to validate
    cur.execute(select)
    tmp_return = cur.fetchall()
    conn.commit()
    conn.close()

    # Return them
    print("Found %d updated resources to validate \n" % len(tmp_return))
    return tmp_return

def updateSchema(resources):
    #Connect to the database
    d_port = config.get('ckan.cprvalidation.postgres_port', None)
    d_pass = config.get('ckan.cprvalidation.cprvalidation_password', None)

    try:
        conn = psycopg2.connect(database="cprvalidation",host="localhost", user="cprvalidation",password=d_pass,port=d_port)
    except Exception as e:
        print(e)
        sys.exit()

    # Fetch all resources from the database
    print("Looking for new resources..")
    cur = conn.cursor()
    cur.execute("""SELECT resource_id, last_updated FROM cprvalidation.status;
        """)
    database_resources = cur.fetchall()
    # These are new resources
    difference_insert = list(set([str(r['id']) for r in resources]) - set(r[0] for r in database_resources))
    difference_update = list(set([str(r['metadata_modified']) for r in resources]) - set(r[1] for r in database_resources))

    insert = """
                INSERT INTO cprvalidation.status values %s
                ON CONFLICT (resource_id) DO
                  UPDATE SET last_updated = %s
                returning *
            ;"""
    update = """
                    UPDATE cprvalidation.status SET last_updated = %s
                    WHERE resource_id = %s AND excepted != TRUE
                    returning *
                ;"""

    '''       Overview of the table order
     (
              package_id character varying NOT NULL,
              resource_id character varying NOT NULL,
              status character varying, -- valid, invalid, pending
              format character varying NOT NULL,
              url character varying,
              url_type character varying,
              datastore_active character varying,
              last_checked character varying,
              last_updated character varying,
              excepted BOOLEAN
              CONSTRAINT status_pkey PRIMARY KEY (resource_id)
            )
    '''
    #For each new resource, add them to the schema and set their status to pending
    count = 0
    for id in difference_insert:
        count += 1
        dict = find(resources,"id",id)
        i = (dict["package_id"],
             dict["id"],
             "pending",
             dict["format"],
             dict["url"],
             dict["url_type"],
             dict["datastore_active"],
             None,
             dict["metadata_modified"],
             )
        u = dict["metadata_modified"]
        cur.execute(insert, (i,u))
    print("Inserted %d new resources to the database \n" % count)

    # # #
    # Update the information for last_updated
    # # #
    count = 0
    for date in difference_update:
        count += 1
        dict = find(resources, "metadata_modified", date)
        i = date
        cur.execute(update, (i, dict["id"]))
    print("Updated %d new resources to the database \n" % count)

    conn.commit()
    conn.close()


def getAllResources():
    response = get_action('package_search')({}, {'rows': 1000000, 'include_private':True})
    local_data = response['results']

    resources = []
    for package in local_data:
        for resource in package["resources"]:
            resource["metadata_modified"] = package["metadata_modified"]
            resources.append(resource)

    return resources

# Simple function that helps us find a value in a list of dicts
def find(lst, key, value):
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return dic
    return None


def validcpr(file_string):
    monthList = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    magic = [4, 3, 2, 7, 6, 5, 4, 3, 2, 1]

    # REGEX Explanation:
    # A or B (A | B)
    # Case A:
    # 6 digits followed by a dash followed by 4 digits
    # Word boundary \b at either end so we wont submatch

    # Case B:
    # 10 digits surrounded by word boundary so we wont submatch
    pattern = re.compile(r"\b(?![\*\.\-\:])\d{6}[-]\d{4}\b(?![\*\.\-\:])|\b(?<![\*\.\-\:])\d{10}\b(?![\*\.\-\:])")

    # Find all match in the file_string
    m = re.findall(pattern, file_string)

    if (len(m) == 0):
        return (False, "No CPR Detected")

    for cpr in m:
        # Since we use word boundaries, remove the trailing spaces
        cpr = cpr.replace(" ", "")
        cpr = cpr.replace("-", "")

        if len(cpr) != 10:
            iscpr = (False, "No CPR Detected")
        elif not cpr.isdigit():
            iscpr = (False, "No CPR Detected")
        elif int(cpr[0:2]) > 31 or int(cpr[0:2]) <= 0:
            iscpr = (False, "No CPR Detected")
        elif int(cpr[2:4]) > 12 or int(cpr[2:4]) <= 0:
            iscpr = (False, "No CPR Detected")
        elif monthList[int(cpr[2:4]) - 1] < int(cpr[0:2]):
            iscpr = (False, "No CPR Detected")
        else:  # All the checks "fail" meaning we might have a CPR number
            sum = 0
            # Do the mod 11 method
            for i in range(0, len(cpr)):
                sum = sum + int(cpr[i:i + 1]) * magic[i]  # Use the magic list to find CPR match
            if sum % 11 == 0:  # If we get mod 11 we have a CPR number
                # Might as well return as we only need 1 match
                return (True, str(cpr))
            else:
                iscpr = (False, "No CPR Detected")

    return iscpr