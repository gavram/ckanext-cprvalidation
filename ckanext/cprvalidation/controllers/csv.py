import os
from ckan.logic import get_action
from ckan.controllers.admin import AdminController

class CprExportController(AdminController):

    def download(self):
        """Uses package_search action to get all datasets in JSON format and transform to CSV"""
        pass