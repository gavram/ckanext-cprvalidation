import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckan.lib.helpers as h
from ckan.plugins.toolkit import Invalid
from logging import getLogger

log = getLogger(__name__)

def verified_validator(value,context):
    if value not in ['true','false','ptrue','pfalse','pending','ppending','initialized']:
        raise Invalid("Invalid verification status")
    return value

class CprvalidationPlugin(plugins.SingletonPlugin, toolkit.DefaultDatasetForm):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IDatasetForm)
    plugins.implements(plugins.IPackageController, inherit=True)
    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'cprvalidation')

    # IPackageController
    def after_create(self,context,pkg_dict):
        #After a dataset is created or updated, this is called
        # At this point the "verified" should be set to "initialized" by the dataset creation process

        verified = "pending"
        #TODO: Why do we do this again, instead of using pkg_dict?
        dataset = get_action('package_show')(context, {'id': pkg_dict['id']})

        if(str(dataset['private']) == 'true'):
            verified = "ppending"

        #If the dataset is already pending, don't trigger an update or we infinite loop
        if(dataset['verified'] != "pending" or dataset['verified'] != 'ppending'):

            #Set the verification status to pending or private pending, make the data private
            dataset['verified'] = verified
            dataset['private'] = 'true'
            try:
                get_action('package_update')(context,dataset)
                log.warn("Changed status of dataset: " + str(dataset['id'] + " to " + str(verified)))
            except:
                log.warn("Something went wrong with the Validation update")


    # IDatasetForm - expanded schema
    def create_package_schema(self):
        # grab the default schema
        schema = super(CprvalidationPlugin, self).create_package_schema()

        schema.update({
            'verified': [verified_validator,
                         toolkit.get_converter('convert_to_extras')],
        })
        return schema

    def update_package_schema(self):
        # grab the default schema
        schema = super(CprvalidationPlugin, self).update_package_schema()

        schema.update({
            'verified': [verified_validator,
                         toolkit.get_converter('convert_to_extras')],
        })
        return schema

    def show_package_schema(self):
        # grab the default schema
        schema = super(CprvalidationPlugin, self).show_package_schema()

        schema.update({
            'verified': [verified_validator,
                         toolkit.get_converter('convert_to_extras')],
        })
        return schema

    def is_fallback(self):
        return True

    def package_types(self):
        return []

    def get_helpers(self):
        return []