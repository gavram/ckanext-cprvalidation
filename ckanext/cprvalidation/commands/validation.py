import sys
import re
from pprint import pprint
import logging
import psycopg2
from ckan.lib.cli import CkanCommand

log = logging.getLogger(__name__)

#TODO: get these values from the config

d_password = "xxx"


class Validation(CkanCommand):
    '''Performs CPR Validation.

    Usage:
        validation initdb
            Creates the database, must have configured the config with the correct password

        validation scan
            Scans ckan for new resources and changes and validates them, run this periodically
    '''
    def initdb(self):
        #For debugging purposes we delete the database everytime
        create_db = '''
            DROP DATABASE cprvalidation;

            CREATE DATABASE cprvalidation
            WITH OWNER = cprvalidation
            ENCODING = 'UTF8'
            TABLESPACE = pg_default
            LC_COLLATE = 'Danish_Denmark.1252'
            LC_CTYPE = 'Danish_Denmark.1252'
            CONNECTION LIMIT = -1;
        '''
        create_schema = '''
            DROP SCHEMA cprvalidation;
            CREATE SCHEMA cprvalidation
            AUTHORIZATION cprvalidation;
        '''

        create_table = '''
            DROP TABLE cprvalidation.status;

            CREATE TABLE cprvalidation.status
            (
              package_id character varying NOT NULL,
              resource_id character varying NOT NULL,
              status character varying, -- valid, invalid, pending
              format character varying NOT NULL,
              last_checked character varying,
              url character varying,
              url_type character varying,
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
            conn = psycopg2.connect(database="cprvalidation", host="localhost", user="cprvalidation", password=d_password,
                                    port="5432")
            print("Connected to the database..")
        except Exception as e:
            print(e)
            sys.exit()

            cur = conn.cursor()
            cur.execute(create_db)
            cur.execute(create_schema)
            cur.execute(create_table)

            conn.commit()
            conn.close()

    def scan(self):
        pass
        #resource_list = getAllResources()  # list of all resources in CKAN

        # Update the database with new resources and / or packages and returns a list of resources which has changed. (link, format)
        #resources_to_check = updateSchema(resource_list)

        #for r in resources_to_check:
        #    validateResource(r)








    def command(self):
        self._load_config()
        print ''

        if len(self.args) == 0:
            self.parser.print_usage()
            sys.exit(1)
        cmd = self.args[0]
        if cmd == 'initdb':
            self.initdb()
        elif cmd == 'scan':
            self.scan()
        else:
            print 'Command %s not recognized' % cmd