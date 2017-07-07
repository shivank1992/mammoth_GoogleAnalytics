__author__ = 'pankaj'

CONFIG_FILE = "GoogleAnalytics.json"

ACCESS_TYPE = 'offline'
PROMPT_TYPE = 'consent'

GA_METRIC_SETS = {
        'desktop': {
            'METRICS': ['ga:sessions', 'ga:percentNewSessions', 'ga:pageviews'],
            'DIMENSIONS': ['ga:date', 'ga:browser', 'ga:operatingSystem', 'ga:deviceCategory', 'ga:pagePath']
        },
        'mobile': {
            'METRICS': ['ga:sessions', 'ga:percentNewSessions', 'ga:pageviews'],
            'DIMENSIONS': ['ga:date', 'ga:browser', 'ga:operatingSystem', 'ga:mobileDeviceInfo', 'ga:deviceCategory',
                           'ga:pagePath']
        }
    }

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

class IDENTITY_FIELDS:
    
    pass


class CONFIG_FIELDS:
    GA_REPORT_TYPE = "ga_report_type"
    
    pass