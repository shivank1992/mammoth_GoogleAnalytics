# encoding: utf-8
import argparse
import logging
import const
import sdk.const as sdkconst
from threep.base import DataYielder
import csv
import httplib2
from oauth2client.client import OAuth2WebServerFlow, OAuth2Credentials
from apiclient.discovery import build
import time
import sys
import string
from apiclient.errors import HttpError

log = logging


class GoogleAnalyticsDataYielder(DataYielder):
    def __init__(self, *args, **kwargs):
        self.knowledge = None
        self.batchId = kwargs.get(sdkconst.KEYWORDS.BATCH_ID)
        del kwargs[sdkconst.KEYWORDS.BATCH_ID]
        super(GoogleAnalyticsDataYielder, self).__init__(*args, **kwargs)


    def get_format_spec(self):
        """
            :return: format spec as a dictionary in the following format:
                {
                    UNIQUE_COLUMN_IDENTIFIER_1: FORMAT_SPEC for column1,
                    UNIQUE_COLUMN_IDENTIFIER_2: FORMAT_SPEC for column2
                    ...
                }
                FORMAT_SPEC examples:
                 for a DATE type column format could be : '%d-%b-%Y', so, it's entry
                 in the spec would look like:
                        COLUMN_IDENTIFIER: '%d-%b-%Y'

            """
        return {}



    def get_data_as_csv(self, file_path):
        """
            :param file_path: file path where csv results has to be saved
            :return: dict object mentioning csv download status, success/failure
            TODO: return dict format to be standardized
        """
        #target = open(file_path, 'wb')
        #writer = csv.writer(target, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)

        #print('\n************************Identity Config********************************\n')

        #print self.identity_config
        #print('\n************************Pulled Data********************************\n')

        #print self.get_ga_data_for_metadata()
        #print('\n***************************MetaData*********************************\n')
        #print(type(self.metadata))
        #print self.metadata
        #print('\n***************************End of MetaData*********************************\n')
        #print(self.start_date,type(self.start_date))
        #print(self.end_date, type(self.start_date))
        #print (self.ds_config.get('ga_report_type'))


        #Get variables from get_ga_data_for_metadata to use in Paging function
        service = self.service
        accounts = service.management().accounts().list().execute()
        firstAccountId = accounts.get('items')[0].get('id')
        webproperties = service.management().webproperties().list(accountId="~all").execute()
        property = webproperties.get('items')[0].get('id')
        profiles = service.management().profiles().list(accountId=firstAccountId,webPropertyId=property).execute()

        # Get Profile Url
        profile_url = profiles.get('items')[0].get('websiteUrl')

        # Get Profile ID
        profile_id = webproperties.get('items')[0].get('defaultProfileId')

        start_date = self.start_date_yyyymmdd
        end_date = self.end_date_yyyymmdd


        # Multiple dates can queried using this type of format.
        date_ranges = [(start_date, end_date)]
        ##date_ranges = [(start_date1, end_date1),(start_date2, end_date2),(start_date3, end_date3)]
        ## Eg : User can query data for the first 15 days of every month using this kind of format.
        # date_ranges = [('2017-01-01','2017-01-15'),
        # ('2017-02-01','2017-02-15'),
        # ('2017-03-01','2017-03-15')]

        #Dimensions from User
        user_dimensions = ",".join(const.GA_METRIC_SETS[self.ds_config.get('ga_report_type')]['DIMENSIONS'])

        #Metrics from User
        user_metrics = ",".join(const.GA_METRIC_SETS[self.ds_config.get('ga_report_type')]['METRICS'])

        # Make a list of Multiple profiles in profile_ids to query multiple profiles in one single query.
        profile_ids = {profile_url: profile_id}

        print('\n************************Data Check before CSV Query********************************\n')

        print(profile_id, date_ranges,user_dimensions,user_metrics, profile_url)
        profile_ids = {profile_url: profile_id}
        #list here for multiple profiles in one single query.


        #Paging Function starts here
        class SampledDataError(Exception):pass
        def main(argv):
            # Try to make a request to the API. Print the results or handle errors.
            try:
                profile_id = profile_ids[profile]
                if not profile_id:
                    print('Could not find a valid profile for this user.')
                else:
                    for start_date, end_date in date_ranges:
                        limit = ga_query(service, profile_id, 0, start_date, end_date, user_metrics,user_dimensions).get('totalResults')
                        for pag_index in xrange(0, limit, 10000):
                            results = ga_query(service, profile_id, pag_index, start_date, end_date,user_metrics,user_dimensions)
                            if results.get('containsSampledData'):
                                raise SampledDataError
                            print_results(results, pag_index, start_date, end_date)


            except TypeError, error:
                # Handle errors in constructing a query.
                print('There was an error in constructing your query' % error)

            except HttpError, error:
                # Handle API errors.
                print('Arg, there was an API error : %s : %s' % (error.resp.status, error._get_reason()))

            except AccessTokenRefreshError:
                # Handle Auth errors.
                print('The credentials have been revoked or expired, please re-run ' 'the application to re-authorize')

            except SampledDataError:
                # force an error if ever a query returns data that is sampled!
                print('Error: Query contains sampled data!')


        def ga_query(service, profile_id, pag_index, start_date, end_date, user_metrics, user_dimensions):
            return service.data().ga().get(
                ids='ga:' + profile_id,
                start_date=start_date,
                end_date=end_date,
                metrics=user_metrics,
                dimensions=user_dimensions,
                sort='-ga:pageviews',
                samplingLevel='HIGHER_PRECISION',
                start_index=str(pag_index + 1),
                max_results=str(pag_index + 10000)).execute()

        def print_results(results, pag_index, start_date, end_date):
            """Prints out the results.
            This prints out the profile name, the column headers, and all the rows of
            data.
            Args:
              results: The response returned from the Core Reporting API.
            """

            # New write header
            if pag_index == 0:
                if (start_date, end_date) == date_ranges[0]:
                    print('Profile Name: %s' % results.get('profileInfo').get('profileName'))
                    columnHeaders = results.get('columnHeaders')
                    cleanHeaders = [str(h['name']) for h in columnHeaders]
                    writer.writerow(cleanHeaders)
                print('Now pulling data from %s to %s.' % (start_date, end_date))

            # Print data table.
            if results.get('rows', []):
                for row in results.get('rows'):
                    for i in range(len(row)):
                        old, new = row[i], str()
                        for s in old:
                            new += s if s in string.printable else ''
                        row[i] = new
                    writer.writerow(row)

            else:
                print('No Rows Found')

            limit = results.get('totalResults')
            print(pag_index, 'of about', int(round(limit, -4)), 'rows.')
            return None



        for profile in sorted(profile_ids):
            with open(file_path, 'wt') as f:

                writer = csv.writer(f, lineterminator='\n')
                if __name__ == '__main__': main(sys.argv)
                f.close()
            print('%s done. Next profile...' % profile)

        print("All profiles done.")

        log.info("file saved at :{0}".format(file_path))
        return {}

    def get_ga_data_for_metadata(self):
        http = httplib2.Http()
        credentials = OAuth2Credentials.from_json(self.identity_config.get('credentials'))
        # identity = Identities.get_by_id()
        http = credentials.authorize(http)
        self.service = build('analytics', 'v3', http=http, cache_discovery=False)
        accounts = self.service.management().accounts().list().execute()
        firstAccountId = accounts.get('items')[0].get('id')
        webproperties = self.service.management().webproperties().list(accountId="~all").execute()
        property = webproperties.get('items')[0].get('id')
        profiles = self.service.management().profiles().list(accountId=firstAccountId,webPropertyId=property).execute()

        #Get Profile Url
        self.profile_url = profiles.get('items')[0].get('websiteUrl')

        #Get Profile ID
        self.profile_id = webproperties.get('items')[0].get('defaultProfileId')

        #time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1347517370))
        #time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1347517370))

        #print(self.start_date, type(self.start_date[0]))
        #print(self.end_date, type(self.end_date))
        self.start_date_yyyymmdd = time.strftime('%Y-%m-%d', time.gmtime((self.start_date[0])))
        self.end_date_yyyymmdd = time.strftime('%Y-%m-%d', time.gmtime((self.end_date)))

        ga_data_for_metadata = self.service.data().ga().get(
            ids="ga:" + self.profile_id,
            dimensions=",".join(const.GA_METRIC_SETS[self.ds_config.get('ga_report_type')]['DIMENSIONS']),
            start_date=self.start_date_yyyymmdd,
            end_date=self.end_date_yyyymmdd,
            max_results=10,
            start_index=1,
            metrics=",".join(const.GA_METRIC_SETS[self.ds_config.get('ga_report_type')]['METRICS'])).execute()

        return ga_data_for_metadata

    def get_metadata(self):


        metadata = []

        col_headers = self.ga_data_for_metadata['columnHeaders']
        for column in col_headers:
            if column['name'] == "ga:date":
                metadata.append({'display_name': 'Date',
                                 'internal_name': 'column_date',
                                 'order': 1,
                                 'type': 'DATE',
                                 'format': '%m-%d-%Y'})
            else:
                internal_name = "column_{0}".format((column['name']. \
                                                     split(':')[1]).lower())
                display_name = column['name'].split(':')[1].capitalize()
                order = col_headers.index(column) + 1
                _type = "NUMERIC"
                if column['dataType'] == "INTEGER" or \
                                column['dataType'] == "FLOAT" or \
                                column['dataType'] == "PERCENT" or \
                                column['dataType'] == "CURRENCY":
                    _format = const.METADATA_DICT[column['dataType']]
                    metadata.append({'display_name': display_name,
                                     'internal_name': internal_name,
                                     'order': order,
                                     'type': _type,
                                     'format': _format})
                elif column['dataType'] == "TIME":
                    _format = const.METADATA_DICT['FLOAT']
                    metadata.append({'display_name': display_name,
                                     'internal_name': internal_name,
                                     'order': order,
                                     'type': _type,
                                     'format': _format})
                elif column['dataType'] == "DATA":
                    _format = None
                    _type = "DATE"
                    metadata.append({'display_name': display_name,
                                     'internal_name': internal_name,
                                     'order': order,
                                     'type': _type,
                                     'format': _format})
                else:
                    _format = None
                    _type = "TEXT"
                    metadata.append({'display_name': display_name,
                                     'internal_name': internal_name,
                                     'order': order,
                                     'type': _type,
                                     'format': _format})
        return metadata


    def _setup(self):
        """
            one time computations required to pull data from third party service.
            Apart from basic variable initialization done in __init__ method of
            same class, all other datapull readiness logic should be here
       """
        ds_config_key = self.config_key
        identity_key = self.identity_key
        self.identity_config = self.storage_handle.get(sdkconst.NAMESPACES.IDENTITIES,
                                                       identity_key)

        self.ds_config = self.storage_handle.get(identity_key, ds_config_key)
        self.ga_data_for_metadata = self.get_ga_data_for_metadata()
        self.metadata = self.get_metadata()


    def reset(self):
        """
            use this method to reset parameters, if needed, before pulling data.
            For e.g., in case, you are using cursors to pull, you may need to reset
            cursor object after sampling rows for metadata computation
            """
        pass

    def describe(self):
        """
            :return: metadata as a list of dictionaries in the following format
                {
                    'internal_name': UNIQUE COLUMN IDENTIFIER,
                    'display_name': COLUMN HEADER,
                    'type': COLUMN DATATYPE -  TEXT/DATE/NUMERIC
               }
        """
        #print('\n***************************MetaData*********************************\n')
        #print(type(self.metadata))
        #print self.metadata
        #print('\n***************************End of MetaData*********************************\n')

        return self.metadata