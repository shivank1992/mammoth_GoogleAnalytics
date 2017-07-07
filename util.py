# encoding: utf-8
import logging
import const
import sdk.const as sdkconst
from threep.base import DataYielder
import csv
import httplib2
from oauth2client.client import OAuth2WebServerFlow, OAuth2Credentials
from apiclient.discovery import build
import time

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

        print('\n************************Identity Config********************************\n')

        print self.identity_config
        print('\n************************Pulled Data********************************\n')

        print self.res
        print('\n***************************MetaData*********************************\n')
        print(type(self.metadata))
        print self.metadata
        print('\n***************************End of MetaData*********************************\n')
        print(self.start_date,type(self.start_date))
        print(self.end_date, type(self.start_date))
        print (self.ds_config.get('ga_report_type'))

        f = open(file_path, 'wt')

        # Wrap file with a csv.writer
        writer = csv.writer(f, lineterminator='\n')

        # Write header.
        # header = [h['display_name'] for h in self.metadata]  # this takes the display name as the column as the header

        #writer.writerow(header)
        #print(''.join('%30s' % h for h in header))

        # Write data table.
        res = self.res
        if res.get('rows', []):
            for row in res.get('rows'):
                writer.writerow(row)
                #print(''.join('%30s' % r for r in row))

                #print('\n')
                #print('Success Data Written to CSV File')

        f.close()
        log.info("file saved at :{0}".format(file_path))
        return {}

    def get_GA_data(self):
        http = httplib2.Http()
        credentials = OAuth2Credentials.from_json(self.identity_config.get('credentials'))
        # identity = Identities.get_by_id()
        http = credentials.authorize(http)
        service = build('analytics', 'v3', http=http, cache_discovery=False)

        webproperties = service.management().webproperties().list(
            accountId="~all").execute()

        profile_id = webproperties.get('items')[0].get('defaultProfileId')

        #time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1347517370))
        #time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1347517370))
        res = service.data().ga().get(
            ids="ga:" + profile_id,
            dimensions=",".join(const.GA_METRIC_SETS[self.ds_config.get('ga_report_type')]['DIMENSIONS']),
            start_date='2017-01-01',
            end_date='yesterday',
            max_results=100,
            start_index=1,
            metrics=",".join(const.GA_METRIC_SETS[self.ds_config.get('ga_report_type')]['METRICS'])).execute()

        return res

    def get_metadata(self):


        metadata = []

        col_headers = self.res['columnHeaders']
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
        self.res = self.get_GA_data()
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
        print('\n***************************MetaData*********************************\n')
        print(type(self.metadata))
        print self.metadata
        print('\n***************************End of MetaData*********************************\n')
        print(self.ds_config)

        return self.metadata