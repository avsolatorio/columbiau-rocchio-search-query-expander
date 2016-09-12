'''
Created on Sep 21, 2012

@author: johnterzis

BingClient takes in an Account Key to its ctor and exposes web search query
method to client that is a wrapper of Bing Search API 1.0

Parameters are standardized based on assignment requirements and query returns
top 10 results only, in JSON format
'''

import logging
from py_bing_search import PyBingWebSearch


class BingClient:
    '''
    classdocs
    '''
    def __init__(self, AccountKey=None):
        '''
        Constructor
        '''

        # enfore pseudo privacy of account key member with __ prefix
        self.__i_accountKey = AccountKey

        if self.__i_accountKey is None:
            logging.error('Account Key is NULL!!!')

    # send a web query to Bing Search API returning top 10 results as json
    def webQuery(self, query, result_num=10):
        # format query based on OData protocol and desired JSON format of results

        full_query = query.replace(' ', '+')
        logging.debug('Sending following URL query: ' + full_query)

        print('%-20s= %s' % ("URL", full_query))

        bing_web = PyBingWebSearch(self.__i_accountKey, full_query, web_only=False)
        first_n_result = bing_web.search(limit=result_num, format='json')

        return first_n_result
