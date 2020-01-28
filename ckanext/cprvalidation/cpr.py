import os
import psycopg2
import sys
import logging
import pylons
import StringIO
from ckan.logic import get_action
from ckan.controllers.admin import AdminController
from ckan.lib.cli import parse_db_config

from ckan.common import config

log = logging.getLogger(__name__)

class CprExportController(AdminController):

    def download(self):
        port = config.get('ckan.cprvalidation.postgres_port', None)
        password = config.get('ckan.cprvalidation.cprvalidation_password',None)
        db_name = config.get('ckan.cprvalidation.cprvalidation_db',None)
        db_config = parse_db_config()
        host = db_config.get('db_host')

        if port != None and password != None:
            try:
                conn = psycopg2.connect(database=db_name, host=host, user="cprvalidation", password=password,
                                    port=port)
            except Exception as e:
                log.warn(e)
                sys.exit()
        else:
            log.warn("Config not setup properly! Missing either postgres_port or cprvalidation_password")
            sys.exit()

        select = """COPY (SELECT * FROM {0}.status) to STDOUT WITH CSV HEADER"""
        cur = conn.cursor()

        #Instead of using an actual file, we use a file-like string buffer
        text_stream = StringIO.StringIO()

        cur.copy_expert(select.format(db_name),text_stream)
        output = text_stream.getvalue()

        #Cleanup after ourselves
        text_stream.close()
        conn.commit()
        conn.close()

        pylons.response.headers['Content-Type'] = 'text/csv;charset=utf-8'
        pylons.response.headers['Content-Disposition'] = 'attachment; filename="cpr_report.csv"'
        return output
