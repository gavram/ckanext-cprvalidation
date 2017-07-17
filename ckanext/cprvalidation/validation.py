# encoding: utf-8
import sys
import re
import os
from pprint import pprint
import logging
import psycopg2
from ckan.logic import get_action
from ckan.lib.cli import CkanCommand
from ckan.common import config


import datetime

log = logging.getLogger(__name__)

#TODO: get these values from the config



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
            print("Setup cprvalidation_passwod in /etc/ckan/default/production.ini")
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

        print("Checking %d resources" % len(resource_list))
        # Update the database with new resources and / or packages and returns a list of resources which has changed. (link, format)
        resources_to_check = updateSchema(resource_list)

        count = 0
        for r in resources_to_check:
            validateResource(r)
            count+= 1

        print("Checked %d resources." % count)


    def command(self):
        self._load_config()
        print(" ")

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
        else:
            print('Command %s not recognized' % cmd)


# # # #
# Helper Functions
# # # #
def processCSV(file_path):
    #TODO: This is probably not the best way to handle a CSV file..
    with open(file_path) as f:
        string = f.read().replace(',', ' ')

    return string

def validateResource(resource):

    d_port = config.get('ckan.cprvalidation.postgres_port', None)
    d_pass = config.get('ckan.cprvalidation.cprvalidation_password', None)


    #TODO: get this information from database
    datastore = False
    filestore = True

    id = resource[1]
    format = "csv"
    file_string = None
    iscpr = None
    if filestore:
        file_path = os.path.join("/var/lib/ckan/default/resources/",
                                 id[0:3], id[3:6], id[6:])

        #Converts the file to a string we can regex and check
        if format == "csv":
            file_string = processCSV(file_path)

        #we got the file_string
        iscpr = validcpr(file_string)
    elif datastore:
        #TODO: Implement this
        pass

    if(file_string != None and iscpr != None):
        if(not iscpr): #If we dont have a CPR in the resource
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
                        SET status='invalid', last_checked= %s
                        WHERE resource_id= %s
                        returning *
                    ;"""

            cur = conn.cursor()
            cur.execute(insert, [current_time,id])
            conn.commit()
            conn.close()

def updateSchema(resources):
    #Connect to the database
    d_port = config.get('ckan.cprvalidation.postgres_port', None)
    d_pass = config.get('ckan.cprvalidation.cprvalidation_password', None)

    try:
        conn = psycopg2.connect(database="cprvalidation",host="localhost", user="cprvalidation",password=d_pass,port=d_port)
        print("Connected..")
    except Exception as e:
        print(e)
        sys.exit()

    # Fetch all resources from the database
    print("Getting resource_ids from database")
    cur = conn.cursor()
    cur.execute("""SELECT resource_id FROM cprvalidation.status;
        """)
    database_resources = cur.fetchall()
    # Take the difference between both sets. (These are new resources) These will then be inserted with a pending flag
    difference = list(set([str(r['id']) for r in resources]) - set(r[0] for r in database_resources))
    insert = """
                insert into cprvalidation.status values %s
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
              CONSTRAINT status_pkey PRIMARY KEY (resource_id)
            )
    '''
    #For each new resource, add them to the schema and set their status to pending
    for i in range(1,len(difference)):
        dict = find(resources,'id',difference[i])
        package = get_action('package_show')({}, {'id': dict["package_id"]})
        #print(package["metadata_modified"])
        p = (dict["package_id"],
             dict["id"],
             "pending",
             dict["format"],
             dict["url"],
             dict["url_type"],
             dict["datastore_active"],
             None,
             package["metadata_modified"]
             )
        print(" ")
        print("INSERT %d / %d" % (i,len(difference)))
        cur.execute(insert, (p,))

    #TODO: this query needs to pick all the formats, and the url_type can also be None if datastore is active
    select = """
                SELECT * FROM cprvalidation.status WHERE format = 'CSV' AND status = 'pending' AND url_type = 'upload';
    """
    #Get the datasets we have to validate
    cur.execute(select)
    tmp_return = cur.fetchall()
    conn.commit()
    conn.close()

    # Return them
    print("Found %d resources to validate" % len(tmp_return))
    return tmp_return

def getAllResources():
    response = get_action('package_search')({}, {'rows': 1000000, 'include_private':True})
    local_data = response['results']

    resources = []
    for package in local_data:
        for resource in package["resources"]:
            resources.append(resource)

    return resources

# Simple function that helps us find a value in a list of dicts
def find(lst, key, value):
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return dic
    return None


def validcpr(file_string):
    monthList = [31,29,31,30,31,30,31,31,30,31,30,31]
    magic = [4,3,2,7,6,5,4,3,2,1]

    pattern = re.compile(r"\d{5,6}[ -]\d{4}|\d{10}")

    m = pattern.search(file_string)
    if m == None:
        return False

    cprGroup = m.group()
    cpr = re.sub('[- ]', '', cprGroup)

    iscpr = True

    if len(cpr) != 10:
        iscpr = False
    elif not cpr.isdigit():
        iscpr = False
    elif int(cpr[0:2]) > 31 or int(cpr[0:2]) <= 0:
        iscpr = False
    elif int(cpr[2:4]) > 12 or int(cpr[2:4]) <= 0:
        iscpr = False
    elif monthList[int(cpr[2:4])-1] < int(cpr[0:2]):
        iscpr = False
    else: #All the checks "fail" meaning we might have a CPR number
        sum = 0
        #Do the mod 11 method
        for i in range(0,len(cpr)):
                sum = sum + int(cpr[i:i+1]) * magic[i] #Use the magic list to find CPR match
        if sum % 11 == 0: #If we get mod 11 we have a CPR number
            iscpr = True #Do something here?
        else:
            iscpr = False

    return iscpr