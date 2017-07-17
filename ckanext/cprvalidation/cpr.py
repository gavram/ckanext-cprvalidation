import os
import psycopg2
import sys
import logging
import pylons
import StringIO
from ckan.logic import get_action
from ckan.controllers.admin import AdminController

from ckan.common import config

log = logging.getLogger(__name__)

class CprExportController(AdminController):

    def download(self):
        port = config.get('ckan.cprvalidation.postgres_port', None)
        password = config.get('ckan.cprvalidation.cprvalidation_password',None)

        if port != None and password != None:
            try:
                conn = psycopg2.connect(database="cprvalidation", host="localhost", user="cprvalidation", password=password,
                                    port=port)
            except Exception as e:
                log.warn(e)
                sys.exit()
        else:
            log.warn("Config not setup properly! Missing either postgres_port or cprvalidation_password")
            sys.exit()

        select = """COPY (SELECT * FROM cprvalidation.status) to STDOUT WITH CSV HEADER"""
        cur = conn.cursor()

        #Instead of using an actual file, we use a file-like string buffer
        text_stream = StringIO.StringIO()

        cur.copy_expert(select,text_stream)
        output = text_stream.getvalue()

        #Cleanup after ourselves
        text_stream.close()
        conn.commit()
        conn.close()

        pylons.response.headers['Content-Type'] = 'text/csv;charset=utf-8'
        pylons.response.headers['Content-Disposition'] = 'attachment; filename="cpr_report.csv"'
        return output
