#
# Copyright 2013 Metamarkets Group Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import division

import simplejson as json

from utils.aggregators import *
from utils.postaggregator import *
from utils.filters import *
from utils.query_utils import *

from twisted.internet import defer
from twisted.web.client import getPage

class Druid(object):

    def __init__(self, url, endpoint):
        self.url = url
        self.endpoint = endpoint
        self.result = None
        self.result_json = None
        self.query_type = None

    @defer.inlineCallbacks
    def post(self, query):
        try:
            data = json.dumps(query)

            if self.url.endswith('/'):
                url = self.url + self.endpoint
            else:
                url = self.url + '/' + self.endpoint

            headers = {'Content-Type': 'application/json'}
            data = yield getPage(url, postdata=data, headers=headers, method="POST")
            defer.returnValue(simplejson.loads(data))
        except Exception, e:
            raise IOError('{0} \n Query is: {1}'.format(
                e, json.dumps(self.query_dict, indent=4)))

    def parse(self):
        if self.result_json:
            res = json.loads(self.result_json)
            return res
        else:
            raise IOError('{Error parsing result: {0} for {1} query'.format(
                self.result_json, self.query_type))

    # --------- Query implementations ---------

    def validate_query(self, valid_parts, args):
        for key, val in args.iteritems():
            if key not in valid_parts:
                raise ValueError(
                    '{0} is not a valid query component.'
                    .format(key) +
                    'The list of valid components is: \n {0}'
                    .format(valid_parts))

    def build_query(self, query_type, args):
        query_dict = {'queryType': query_type}

        for key, val in args.iteritems():
            if key == "aggregations":
                query_dict[key] = build_aggregators(val)
            elif key == "postAggregations":
                query_dict[key] = build_post_aggregators(val)
            elif key == "filter":
                query_dict[key] = build_filter(val)
            else:
                query_dict[key] = val

        self.query_dict = query_dict
        self.query_type = query_type

    def top_n(self, **args):
        valid_parts = [
            'dataSource', 'granularity', 'filter', 'aggregations',
            'postAggregations', 'intervals', 'dimension', 'threshold',
            'metric'
        ]
        self.validate_query(valid_parts, args)
        self.build_query('topN', args)
        return self.post(self.query_dict)

    def timeseries(self, **args):
        valid_parts = [
            'dataSource', 'granularity', 'filter', 'aggregations',
            'postAggregations', 'intervals'
        ]
        self.validate_query(valid_parts, args)
        self.build_query('timeseries', args)
        return self.post(self.query_dict)

    def group_by(self, **args):
        valid_parts = [
            'dataSource', 'granularity', 'filter', 'aggregations',
            'postAggregations', 'intervals', 'dimensions'
        ]
        self.validate_query(valid_parts, args)
        self.build_query('groupBy', args)
        return self.post(self.query_dict)

    def segment_metadata(self, **args):
        valid_parts = ['dataSource', 'intervals']
        self.validate_query(valid_parts, args)
        self.build_query('segmentMetaData', args)
        return self.post(self.query_dict)

    def time_boundary(self, **args):
        valid_parts = ['dataSource']
        self.validate_query(valid_parts, args)
        self.build_query('timeBoundary', args)
        return self.post(self.query_dict)
