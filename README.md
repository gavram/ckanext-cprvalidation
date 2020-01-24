# ckanext-cprvalidation
Validates resources for the Danish national open data platform
http://portal.opendata.dk

Installing
NB! This module is developed on CKAN v2.6., compatibility with other version is not ensured

### Activate virtualenv
```
source /usr/lib/ckan/default/bin/activate
cd /usr/lib/ckan/default/src
git clone git@github.com:NicolaiMogensen/ckanext-cprvalidation.git
cd ckanext-cprvalidation
```

### Install Extension
```
python setup.py develop

```
### Enable plugin in configuration
```
 sudo nano /etc/ckan/default/production.ini
 ckan.plugins = datastore ... cprvalidation
```
### Add database settings to production.ini
```
ckan.cprvalidation.postgres_password = "Postgres password here"
ckan.cprvalidation.cprvalidation_db = "Database name for validation"
ckan.cprvalidation.cprvalidation_password = "Password you will be using for the dedicated user"
ckan.cprvalidation.postgres_port = "The port postgres is running, default is 5432"
ckan.cprvalidation.apikey = "A CKAN API key that can view private resources"
ckan.cprvalidation.email = "Email that the OS should send report to. "mail" must be setup"
```

### Create user "cprvalidation"
```
sudo -u postgres psql
CREATE ROLE cprvalidation WITH LOGIN ENCRYPTED PASSWORD 'xxx';
```
NB: The password must be the same as the one entered in your config

## Usage

### Init the database
```
paster --plugin=ckanext-cprvalidation validation initdb --config=/etc/ckan/default/production.ini
```

### Setup a CRON job to scan at regular intervals.
```
*/30 * * * * cd /usr/lib/ckan/default/src/ckanext-cprvalidation && /usr/lib/ckan/default/bin/python /usr/lib/ckan/default/bin/paster plugin=ckanext-cprvalidation validation scan --config=/etc/ckan/default/production.ini
```
### Add exceptions to the database
```
paster --plugin=ckanext-cprvalidation validation addexception "Package_id" --config=/etc/ckan/default/production.ini
```
