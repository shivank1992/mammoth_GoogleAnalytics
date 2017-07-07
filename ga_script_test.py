# encoding: utf-8

from apiclient.discovery import build

__author__ = ''

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
from oauth2client import client
import flask
from oauth2client.client import OAuth2WebServerFlow, OAuth2Credentials
import argparse
import httplib2
import csv

# Insert your import statements here
from runtime_import.libs.GoogleAnalytics.util import GoogleAnalyticsDataYielder
identity_config = {
    u'credentials': u'{"_module": "oauth2client.client",'
                    u' "scopes": ["https://www.googleapis.com/auth/userinfo.email",'
                    u' "https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/analytics.readonly"],'
                    u' "token_expiry": "2017-06-21T11:48:13Z", '
                    u'"id_token": {"picture": "https://lh3.googleusercontent.com/-3TcAbcrIJF0/AAAAAAAAAAI/AAAAAAAAAAA/AAyYBF4ggrHUQm7mR6GMAlGgua8LEvnpAg/s96-c/photo.jpg", "aud": "768649915969-6gqqnebt6iq4cpnlq7i8hu9evph2fmq4.apps.googleusercontent.com", "family_name": "Garg", "iss": "https://accounts.google.com", "email_verified": true, "name": "Shivank Garg", "at_hash": "o1WMhaN4nJou4rzO77kN2g", "given_name": "Shivank", "exp": 1498045694, "azp": "768649915969-6gqqnebt6iq4cpnlq7i8hu9evph2fmq4.apps.googleusercontent.com", "iat": 1498042094, "locale": "en", "email": "shivank.phone@gmail.com", "sub": "110282609574854913466"}, "user_agent": null, "access_token": "ya29.GltwBKgtCpkVxfxt0hdhdiHIKo9qPUSl2Beb4C-ZRCM2o2KpeYBWyRcT-dN8AkWPOZDgIROX_eoPQo79F9wiTe7WMMWNkvWOpQb9tjakSkyUU0jBLHc_78APX1fL",'
                    u' "token_uri": "https://www.googleapis.com/oauth2/v4/token", "invalid": false, '
                    u'"token_response": {"access_token": "ya29.GltwBKgtCpkVxfxt0hdhdiHIKo9qPUSl2Beb4C-ZRCM2o2KpeYBWyRcT-dN8AkWPOZDgIROX_eoPQo79F9wiTe7WMMWNkvWOpQb9tjakSkyUU0jBLHc_78APX1fL", "token_type": "Bearer", "expires_in": 3600, "refresh_token": "1/QzGOmTSxlZ8obGki0nJGugwPElQe4SfJUhytfQ2BTxkl2hBm_bMSUdj8GxwoIZV1", "id_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6IjZlMzYwOGZmOWQ0MDAwYThmYWNmMDYxMmNlOWFmYTVjN2I1MjczNjgifQ.eyJhenAiOiI3Njg2NDk5MTU5NjktNmdxcW5lYnQ2aXE0Y3BubHE3aThodTlldnBoMmZtcTQuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJhdWQiOiI3Njg2NDk5MTU5NjktNmdxcW5lYnQ2aXE0Y3BubHE3aThodTlldnBoMmZtcTQuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJzdWIiOiIxMTAyODI2MDk1NzQ4NTQ5MTM0NjYiLCJlbWFpbCI6InNoaXZhbmsucGhvbmVAZ21haWwuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImF0X2hhc2giOiJvMVdNaGFONG5Kb3U0cnpPNzdrTjJnIiwiaXNzIjoiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tIiwiaWF0IjoxNDk4MDQyMDk0LCJleHAiOjE0OTgwNDU2OTQsIm5hbWUiOiJTaGl2YW5rIEdhcmciLCJwaWN0dXJlIjoiaHR0cHM6Ly9saDMuZ29vZ2xldXNlcmNvbnRlbnQuY29tLy0zVGNBYmNySUpGMC9BQUFBQUFBQUFBSS9BQUFBQUFBQUFBQS9BQXlZQkY0Z2dySFVRbTdtUjZHTUFsR2d1YThMRXZucEFnL3M5Ni1jL3Bob3RvLmpwZyIsImdpdmVuX25hbWUiOiJTaGl2YW5rIiwiZmFtaWx5X25hbWUiOiJHYXJnIiwibG9jYWxlIjoiZW4ifQ.KrP8dCM23BWQHajbk1W4u8rE7DzKY79VLhyVDV3C1KmYWQYNLRJmIqVb0AgLGlmb0VZmfuKnz1h-ekhCEh9SgrwmwcXCZ-bQtXC7XnoVYTlTj2lFnWAou80y2emJYrdm9YNHKvqkMsy27aGnSLNdVeuQplAAtcZm0eGU_u0em0-DMEnk5rA1iWNsNHbhxga0VQDdKr56eFrk-OH0nd8q0k1P5AA97OyBHj84ib6BvDbwU9L02UqFy5XetQx-aQfRaGygzAlH9ACkHVClnxOLhPij8tjBZHptYqEA-tzJjfmkg0pngyKjQkKV_uQatM9qTjlOWvwsfUK3jEfz2DaGFw"}, "client_id": "768649915969-6gqqnebt6iq4cpnlq7i8hu9evph2fmq4.apps.googleusercontent.com", "token_info_uri": "https://www.googleapis.com/oauth2/v3/tokeninfo", "client_secret": "BqLLypXr0lN5Pd_r_k25U9_k", '
                    u'"revoke_uri": "https://accounts.google.com/o/oauth2/revoke", '
                    u'"_class": "OAuth2Credentials", '
                    u'"refresh_token": "1/QzGOmTSxlZ8obGki0nJGugwPElQe4SfJUhytfQ2BTxkl2hBm_bMSUdj8GxwoIZV1",'
                    u' "id_token_jwt": "eyJhbGciOiJSUzI1NiIsImtpZCI6IjZlMzYwOGZmOWQ0MDAwYThmYWNmMDYxMmNlOWFmYTVjN2I1MjczNjgifQ.eyJhenAiOiI3Njg2NDk5MTU5NjktNmdxcW5lYnQ2aXE0Y3BubHE3aThodTlldnBoMmZtcTQuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJhdWQiOiI3Njg2NDk5MTU5NjktNmdxcW5lYnQ2aXE0Y3BubHE3aThodTlldnBoMmZtcTQuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJzdWIiOiIxMTAyODI2MDk1NzQ4NTQ5MTM0NjYiLCJlbWFpbCI6InNoaXZhbmsucGhvbmVAZ21haWwuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImF0X2hhc2giOiJvMVdNaGFONG5Kb3U0cnpPNzdrTjJnIiwiaXNzIjoiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tIiwiaWF0IjoxNDk4MDQyMDk0LCJleHAiOjE0OTgwNDU2OTQsIm5hbWUiOiJTaGl2YW5rIEdhcmciLCJwaWN0dXJlIjoiaHR0cHM6Ly9saDMuZ29vZ2xldXNlcmNvbnRlbnQuY29tLy0zVGNBYmNySUpGMC9BQUFBQUFBQUFBSS9BQUFBQUFBQUFBQS9BQXlZQkY0Z2dySFVRbTdtUjZHTUFsR2d1YThMRXZucEFnL3M5Ni1jL3Bob3RvLmpwZyIsImdpdmVuX25hbWUiOiJTaGl2YW5rIiwiZmFtaWx5X25hbWUiOiJHYXJnIiwibG9jYWxlIjoiZW4ifQ.KrP8dCM23BWQHajbk1W4u8rE7DzKY79VLhyVDV3C1KmYWQYNLRJmIqVb0AgLGlmb0VZmfuKnz1h-ekhCEh9SgrwmwcXCZ-bQtXC7XnoVYTlTj2lFnWAou80y2emJYrdm9YNHKvqkMsy27aGnSLNdVeuQplAAtcZm0eGU_u0em0-DMEnk5rA1iWNsNHbhxga0VQDdKr56eFrk-OH0nd8q0k1P5AA97OyBHj84ib6BvDbwU9L02UqFy5XetQx-aQfRaGygzAlH9ACkHVClnxOLhPij8tjBZHptYqEA-tzJjfmkg0pngyKjQkKV_uQatM9qTjlOWvwsfUK3jEfz2DaGFw"}',
    u'config_key': u'3vfvo1v3kralvpfdg4hq2qi12rhavtasinj0liid',
    u'name': u'Shivank Garg (shivank.phone@gmail.com)'}


if __name__ == '__main__':
    http = httplib2.Http()
    credentials = OAuth2Credentials.from_json(identity_config.get('credentials'))
    # identity = Identities.get_by_id()


    http = credentials.authorize(http)
    service = build('analytics', 'v3', http=http)


    print("Account Information \n")
    accounts = service.management().accounts().list().execute()
    #print (accounts)
    #print("\n")

    print("Account Name \n")
    account_name = accounts.get('items')[0].get('name')
    print(account_name)
    print("\n")


    print("First Account ID \n")
    s_firstAccountId = accounts.get('items')[0].get('id')
    print(s_firstAccountId)
    print("\n")

    print("Web Properties \n")
    s_webproperties = service.management().webproperties().list(
        accountId="~all").execute()
    #print(s_webproperties)
    print("\n")

    property = s_webproperties.get('items')[0].get('id')
    profiles = service.management().profiles().list(
        accountId=s_firstAccountId,
        webPropertyId=property).execute()

    print("Profile URL \n")
    profile_url = profiles.get('items')[0].get('websiteUrl')
    print(profile_url)
    print("\n")

    print("Profile ID \n")
    profile_id = s_webproperties.get('items')[0].get('defaultProfileId')
    print profile_id
    print("\n")


    print (account_name + " - " + profile_url)


    for property in s_webproperties.get('items', []):
        print 'Account ID         = %s' % property.get('accountId')
        print 'Property ID    = %s' % property.get('id')
        print 'Property Name  = %s' % property.get('name')
        print 'Property Profile Count = %s' % property.get('profileCount')
        print 'Property Industry Vertical = %s' % property.get('industryVertical')
        print 'Property Internal Id = %s' % property.get('internalWebPropertyId')
        print 'Property Level = %s' % property.get('level')
        if property.get('websiteUrl'):
            print 'Property URL        = %s' % property.get('websiteUrl')
        print 'Created            = %s' % property.get('created')
        print 'Updated            = %s' % property.get('updated')

    print("$$$$$$$$$$$$$/n")

    '''data = service.data().ga().get(
        ids='ga:' + profile_id,
        start_date='2012-04-01',
        end_date='2012-08-01',
        dimensions='ga:source,ga:keyword',
        sort='-ga:sessions,ga:source',
        metrics='ga:sessions').execute()
    print data'''

    GA_METRIC_SETS = {
        'DESKTOP': {
            'METRICS': ['ga:sessions', 'ga:percentNewSessions', 'ga:pageviews'],
            'DIMENSIONS': ['ga:date', 'ga:browser', 'ga:operatingSystem', 'ga:deviceCategory', 'ga:pagePath']
        },
        'MOBILE': {
            'METRICS': ['ga:sessions', 'ga:percentNewSessions', 'ga:pageviews'],
            'DIMENSIONS': ['ga:date', 'ga:browser', 'ga:operatingSystem', 'ga:mobileDeviceInfo', 'ga:deviceCategory',
                           'ga:pagePath']
        }
    }


    '''metadata = []

    METADATA_DICT = {
        'INTEGER': {'numtype': "int",
                    'is_percentage': "false",
                    'currency_symbol': None},
        'FLOAT': {'numtype': "float",
                  'is_percentage': "false",
                  'currency_symbol': None},
        'PERCENT': {'numtype': "float",
                    'is_percentage': "true",
                    'currency_symbol': None},
        'CURRENCY': {'numtype': "float",
                     'is_percentage': "false",
                     'currency_symbol': "$"},
    }
    d = ",".join(GA_METRIC_SETS['DESKTOP']['DIMENSIONS'])
    print d
    #print (GA_METRIC_SETS['DESKTOP']['DIMENSIONS'])

    #print(GA_METRIC_SETS['DESKTOP']['METRICS'])
    #log.info(tp_ds.feed_info)
    #feed_info = json.loads(data)
    res = service.data().ga().get(
        ids="ga:"+profile_id,
        dimensions=",".join(GA_METRIC_SETS['DESKTOP']['DIMENSIONS']),
        start_date='2017-01-01',
        end_date='yesterday',
        max_results=10,
        start_index=1,
        metrics=",".join(GA_METRIC_SETS['DESKTOP']['METRICS'])).execute()



    print('\n*********************************************************************\n')
    print res
    print('\n*********************************************************************\n')



    col_headers = res['columnHeaders']
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
                _format = METADATA_DICT[column['dataType']]
                metadata.append({'display_name': display_name,
                                 'internal_name': internal_name,
                                 'order': order,
                                 'type': _type,
                                 'format': _format})
            elif column['dataType'] == "TIME":
                _format = METADATA_DICT['FLOAT']
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
    print metadata'''



    #print(service_data)
    webproperties = service.management().webproperties().list(
        accountId="~all").execute()

    profile_id = webproperties.get('items')[0].get('defaultProfileId')

    metadata = []
    METADATA_DICT = {
        'INTEGER': {'numtype': "int",
                    'is_percentage': "false",
                    'currency_symbol': None},
        'FLOAT': {'numtype': "float",
                  'is_percentage': "false",
                  'currency_symbol': None},
        'PERCENT': {'numtype': "float",
                    'is_percentage': "true",
                    'currency_symbol': None},
        'CURRENCY': {'numtype': "float",
                     'is_percentage': "false",
                     'currency_symbol': "$"},
    }

    res = service.data().ga().get(
        ids="ga:" + profile_id,
        dimensions=",".join(GA_METRIC_SETS['DESKTOP']['DIMENSIONS']),
        start_date='2017-01-01',
        end_date='yesterday',
        max_results=10,
        start_index=1,
        metrics=",".join(GA_METRIC_SETS['DESKTOP']['METRICS'])).execute()

    col_headers = res['columnHeaders']
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
                _format = METADATA_DICT[column['dataType']]
                metadata.append({'display_name': display_name,
                                 'internal_name': internal_name,
                                 'order': order,
                                 'type': _type,
                                 'format': _format})
            elif column['dataType'] == "TIME":
                _format = METADATA_DICT['FLOAT']
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

    print(type(metadata))
    print metadata
    print('\n*********************************************************************\n')
    print (type(metadata))

    print res
    print('\n*********************************************************************\n')
    print(type(metadata))

    f = open("/home/shivank/Desktop/mammoth/mylist.csv", 'wt')

    # Wrap file with a csv.writer
    writer = csv.writer(f, lineterminator='\n')

    # Write header.
    header = [h['display_name'] for h in metadata]  # this takes the display name as the column as the header

    writer.writerow(header)
    print(''.join('%30s' % h for h in header))

    # Write data table.
    if res.get('rows', []):
        for row in res.get('rows'):
            writer.writerow(row)
            #print(''.join('%30s' % r for r in row))

            print('\n')
            print('Success Data Written to CSV File')

    f.close()

    '''import pandas
    pd = pandas.DataFrame(res)
    pd.to_csv("/home/shivank/Desktop/mammoth/mylist.csv", encoding='utf-8')'''

