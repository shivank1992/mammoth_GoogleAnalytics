from apiclient.discovery import build

__author__ = 'shivank'

import os
import const
import json
import  logging as log
import sdk.const as sdkconst
from sdk.const import COMMON_CONFIG_FIELDS, \
    COMMON_IDENTITY_FIELDS, NAME, VALUE
from const import CONFIG_FIELDS, IDENTITY_FIELDS
from threep.base import ThreePBase
from sdk.utils import get_key_value_label, make_kv_list
import urllib2


from oauth2client.client import OAuth2WebServerFlow, OAuth2Credentials
import argparse
import httplib2

# Insert your import statements here
from runtime_import.libs.GoogleAnalytics.util import GoogleAnalyticsDataYielder

# End of import statements
OAUTH_SAVE_URL = 'http://localhost:6346/sandbox?integration_key=GoogleAnalytics'
TOKEN_REQUEST_URL = 'https://accounts.google.com/o/oauth2/token'
SCOPE = ['https://www.googleapis.com/auth/analytics.readonly',
         'https://www.googleapis.com/auth/userinfo.profile',
         'https://www.googleapis.com/auth/userinfo.email']


         #'https://www.googleapis.com/auth/userinfo.email'+
         #'https://www.googleapis.com/auth/userinfo.profile')

flow = OAuth2WebServerFlow(
            client_id='768649915969-6gqqnebt6iq4cpnlq7i8hu9evph2fmq4.apps.googleusercontent.com',
            client_secret='BqLLypXr0lN5Pd_r_k25U9_k',
            scope=SCOPE,
            redirect_uri='https://redirect.mammoth.io/redirect/oauth2')

flow.params['access_type'] = const.ACCESS_TYPE
flow.params['prompt'] = const.PROMPT_TYPE



class GoogleAnalyticsManager(ThreePBase):
    """
    This is main class using which Mammoth framework interacts with third party API
    plugin (referred as API hereinafter). Various responsibilities of this class
    is to manage/update identities, ds_configs and few more interfaces to handle API logic

    """

    def __init__(self, storage_handle, api_config):
        
        self.config_file = "/".join([os.path.dirname(__file__), const.CONFIG_FILE])
        super(GoogleAnalyticsManager, self).__init__(storage_handle, api_config)


    def get_identity_spec(self, auth_spec):
        """
           This function is called to render the form on the authentication screen. It provides the render specification to
        the frontend.

        In the simplest case just return the provided auth_spec parameter.
        """

        oauth_url = flow.step1_get_authorize_url()
        oauth_save_url = "http://localhost:6346/sandbox?integration_key=googleanalytics"
        auth_spec["AUTH_URL"] = oauth_url + "&state=" + urllib2.quote(oauth_save_url)
        return auth_spec

    def get_identity_config_for_storage(self, params=None):
        """
        :param params: dict, required to generate identity_config dict object for storage
        :return: newly created identity_config. The value obtained in params is
        a dictionary that should contain following keys:
        
        """
        # create an identity dictionary and store this with the storage handle.
        #auth_code['credentials'] = auth_spec.access_token
        auth_code = params.get("code")
        # Get access token, refresh token
        credentials = flow.step2_exchange(auth_code)

        #print('*********************************************************************')
        credentials_json  = credentials.to_json()
        credentials_data = json.loads(credentials_json)
        #print(type(credentials_data))
        #print(credentials_data)
        #print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        #print(credentials_data["id_token"]["name"])
        #print('*********************************************************************')

        identity_config = {
            'credentials': credentials_json
        }
        if params.get(COMMON_IDENTITY_FIELDS.NAME):
            identity_config[COMMON_IDENTITY_FIELDS.NAME] = params.get(
                COMMON_IDENTITY_FIELDS.NAME)
        else:
            identity_config[sdkconst.COMMON_IDENTITY_FIELDS.NAME] = credentials_data["id_token"]["name"]+" ("+credentials_data["id_token"]["email"]+")"
        return identity_config

    def validate_identity_config(self, identity_config):
        """
            :param identity_config:
            :return: True/False: whether the given identity_config is valid or not
        """
        return True

    def format_identities_list(self, identity_list):
        """
        :param identity_list: all the existing identities, in the
        following format:
            {
                IDENTITY_KEY_1: identity_config_1,
                IDENTITY_KEY_2: identity_config_2,
                ...
            }
        :return:Returns extracted list of  all identities, in the following format:
          [
            {
                name: DISPLAY_NAME_FOR_IDENTITY_1
                value: IDENTITY_KEY_1
            },
            {
                name: DISPLAY_NAME_FOR_IDENTITY_2
                value: IDENTITY_KEY_2
            },
            ...

          ]
        """
        # using make_kv_list method here, You can use your own logic.

        formatted_list = make_kv_list(identity_list, sdkconst.FIELD_IDS.VALUE,
                                       sdkconst.FIELD_IDS.NAME)
        return formatted_list


    def delete_identity(self, identity_config):
        """
            put all the logic here you need before identity deletion and
            if identity can be deleted, return True else False
            returning true will delete the identity from the system.

            :param identity_config: identity
            :return:
        """
        return True

    def get_ds_config_spec(self, ds_config_spec,
                           identity_config, params=None):
        """
            :param ds_config_spec: ds_config_spec from json spec.
            :param identity_config: corresponding identity object for which
                ds_config_spec are being returned
            :param params: additional parameters if any
            :return:  ds_config_spec.
            Any dynamic changes to ds_config_spec, if required, should be made here.
        """
        '''print('$$$$$$$$$$$$%%%%%%%%%%%%###############')
        print(identity_config)
        http = httplib2.Http()
        credentials = OAuth2Credentials.from_json(identity_config.get('credentials'))'''
        # identity = Identities.get_by_id()


        '''http = credentials.authorize(http)
        service = build('analytics', 'v3', http=http)
        service_data = service.data().ga().get(
            ids='ga:' + '40034972',
            start_date='2012-04-01',
            end_date='2012-08-01',
            metrics='ga:sessions',
            dimensions='ga:date').execute()'''




        return ds_config_spec

    def get_ds_config_for_storage(self, params=None):
        """
        :param params: dict, required to generate ds_config dict object for storage
        :return: newly created ds_config. The value obtained in params is
        a dictionary that should contain following keys:
             ga_report_type,
        
        """

        ds_config = {
            CONFIG_FIELDS.GA_REPORT_TYPE: params.get(CONFIG_FIELDS.GA_REPORT_TYPE)
        }

        return ds_config

    def format_ds_configs_list(self, ds_config_list, params=None):
        """
            :param ds_config_list: all the existing ds_configs, in the
            following format:
                {
                    CONFIG_KEY_1: ds_config_1,
                    CONFIG_KEY_2: ds_config_2,
                    ...
                }
            :param params: Additional parameters, if any.
            :return:Returns extracted list of  all ds_configs, in the following format:
              [
                {
                    name: DISPLAY_NAME_FOR_CONFIG_1
                    value: CONFIG_KEY_1
                },
                {
                    name: DISPLAY_NAME_FOR_CONFIG_2
                    value: CONFIG_KEY_2
                },
                ...
        """

        formatted_list = make_kv_list(ds_config_list, sdkconst.VALUE, sdkconst.NAME)
        return formatted_list


    def is_connection_valid(self, identity_config, ds_config=None):
        """
            :param identity_key:
            :param ds_config_key:
            :return: Checks weather the connection specified by provided identity_key and ds_config_key is valid or not. Returns True if valid,
                     False if invalid
        """
        return True

    def sanitize_identity(self, identity):
        """
            update identity object with some dynamic information you need to fetch
            everytime from server. for e.g. access_token in case of OAUTH
            :param identity:
            :return:
        """
        return identity

    def validate_ds_config(self, identity_config, ds_config):
        """
            :param identity_config: identity object
            :param ds_config: ds_config object
            :return: dict object with a mandatory key "is_valid",
            whether the given ds_config is valid or not
        """
        return {'is_valid':True}

    def get_data(self, identity_key, config_key, start_date=None,
                 end_date=None,
                 batch_id=None, storage_handle=None, api_config=None):
        """

        :param self:
        :param identity_key:
        :param config_key:
        :param start_date:
        :param end_date:
        :param batch_id: TODO - replace it with a dict
        :param storage_handle:
        :param api_config:
        :return: instance of DataYielder class defined in util.py
        """



        return GoogleAnalyticsDataYielder(storage_handle,
                    api_config,
                    identity_key,
                    config_key,
                    start_date, end_date, batch_id=batch_id)

    def get_display_info(self, identity_config, ds_config):
        """
            :param self:
            :param identity_config:
            :param ds_config:
            :return: dict object containing user facing information extracted from
             the given identity_config and ds_config.
        """
        pass

    def sanitize_ds_config(self, ds_config):
        """
            :param ds_config:
            :return:

            update ds_config object with some dynamic information you need to update
            every time from server.
        """
        return ds_config

    def augment_ds_config_spec(self, identity_config, params):
        """
            :param params: dict object containing subset ds_config parameters
            :param identity_config:
            :return: dict in the form : {field_key:{property_key:property_value}}
            this method is used to update/augment ds_config_spec with some dynamic
            information based on inputs received
        """
        return {}

    def update_ds_config(self, ds_config, params):
        """
            :param ds_config:
            :param params: dict object containing information required to update ds_config object
            :return: updated ds_config dict
        """
        return ds_config

    def if_identity_exists(self, existing_identities, new_identity):
        """
            :param existing_identities: dict of existing identities
            :param new_identity: new identity dict
            :return: True/False if the new_identity exists already
            in  existing_identities

        """
        return False

    def get_data_sample(self, identity_config, ds_config):
        """
            :param identity_config:
            :param ds_config:
            :return: data sample in the following format:
            {
                "metadata": [],
                "rows": []
            }

            metadata : metadata as a list of dictionaries in the following format
                {
                    'internal_name': UNIQUE COLUMN IDENTIFIER,
                    'display_name': COLUMN HEADER,
                    'type': COLUMN DATATYPE -  TEXT/DATE/NUMERIC
               }

        """
        return {}

    def list_profiles(self, identity_config):
        """
            :param identity_config: for which profiles have to be returned

            :return:Returns list of  all profiles for a given identity_config,
            in the following format:
              [
                {
                    name: DISPLAY_NAME_FOR_PROFILE_1
                    value: PROFILE_IDENTIFIER_1
                },
                {
                    name: DISPLAY_NAME_FOR_PROFILE_2
                    value: PROFILE_IDENTIFIER_2
                },
                ...

              ]
        """
        http = httplib2.Http()
        credentials = OAuth2Credentials.from_json(identity_config.get('credentials'))
        # identity = Identities.get_by_id()


        http = credentials.authorize(http)
        service = build('analytics', 'v3', http=http, cache_discovery=False)
        #print('*********************************************************************')
        #print(build('analytics', 'v3', http=http))
        #print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ \n')

        #accounts = service.management().accounts().list().execute()
        #ids = []
        #if accounts.get('items'):
         #   for account in accounts['items']:
         #       ids.append({'name':account.get('name', 'untitled profile'), 'id': account.get('id')})
        #print(accounts.get('items'))
        #return ids



        # Get a list of all Google Analytics accounts for this user
        accounts = service.management().accounts().list().execute()
        ga_profiles=[]
        if accounts.get('items'):
            # Get the first Google Analytics account.
            account = accounts.get('items')[0].get('id')
            account_name = accounts.get('items')[0].get('name')

            # Get a list of all the properties for the first account.
            properties = service.management().webproperties().list(
                accountId="~all").execute()

            if properties.get('items'):
                # Get the first property id.
                property = properties.get('items')[0].get('id')

                # Get a list of all views (profiles) for the first property.
                profiles = service.management().profiles().list(
                    accountId=account,
                    webPropertyId=property).execute()

                if profiles.get('items'):
                    # return the first view (profile) url
                    profile_url = profiles.get('items')[0].get('websiteUrl')
                    ga_profiles.append({'name':(account_name + " - " + profile_url), 'value' : profile_url})
                    #print (ga_profiles)
                    return ga_profiles


    def delete_ds_config(self, identity_config, ds_config):
        """
            :param identity_config:
            :param ds_config:
            :return: delete status

            put all the pre deletion logic here for ds_config and
            if ds_config can be deleted, return True else False
            returning true will delete the ds_config from the system
        """
        return True