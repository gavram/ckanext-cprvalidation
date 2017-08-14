import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckan.lib.helpers as h
from ckan.plugins.toolkit import Invalid
from logging import getLogger
from ckan.logic import get_action


log = getLogger(__name__)

def verified_validator(value,context):
    #This does not work when the initial value for verified is not set
    if value not in ['true','false','ptrue','pfalse','pending','ppending','initialized']:
        #raise Invalid("Invalid verification status: " + value)
        return value
    return value

def validate_package(context,pkg_dict):
    # After a dataset is created or updated, this is called
    # At this point the "verified" should be set to "initialized" by the dataset creation process

    # TODO: Why do we do this again, instead of using pkg_dict?
    dataset = get_action('package_show')(context, {'id': pkg_dict['id']})

    # If the dataset was private before, we should not make it public after verification
    if (str.lower(str(dataset['private'])) == 'true' and dataset['verified'] != 'pending'):
        dataset['verified'] = 'ppending'
    else:
        dataset['verified'] = 'pending'

    dataset['private'] = 'true'
    dataset['update_trigger'] = 'false'

    try:
        #This will not trigger the next after_update
        get_action('package_update')(context, dataset)
        log.warn("Changed status of dataset: " + str(dataset['id'] + " to " + str(dataset['verified'])))

    except Exception as e:
        log.exception(e)
        log.warn("Something went wrong with the Validation update")


class CprvalidationPlugin(plugins.SingletonPlugin, toolkit.DefaultDatasetForm):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IDatasetForm)
    plugins.implements(plugins.IRoutes, inherit=True)

    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'cprvalidation')

    # IDatasetForm - expanded schema
    def create_package_schema(self):
        # grab the default schema
        schema = super(CprvalidationPlugin, self).create_package_schema()

        schema.update({
            'verified': [toolkit.get_validator('ignore_missing'),
                         toolkit.get_converter('convert_to_extras')],
        })
        return schema

    def update_package_schema(self):
        # grab the default schema
        schema = super(CprvalidationPlugin, self).update_package_schema()

        schema.update({
            'verified': [toolkit.get_validator('ignore_missing'),
                         toolkit.get_converter('convert_to_extras')],
        })
        return schema

    def show_package_schema(self):
        # grab the default schema
        schema = super(CprvalidationPlugin, self).show_package_schema()

        schema.update({
            'verified': [toolkit.get_converter('convert_from_extras'),
                         toolkit.get_validator('ignore_missing')],
        })
        return schema

    def is_fallback(self):
        return True

    def package_types(self):
        return []

    def get_helpers(self):
        return []

    '''IRoutes Adds download button to the admin page'''
    def before_map(self,map):
        cpr_ctrl = 'ckanext.cprvalidation.cpr:CprExportController'
        map.connect('download cpr report','/download/cprreport',controller=cpr_ctrl,action='download')
        return map
