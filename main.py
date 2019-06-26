'''
load flow data from an elasticsearch database, enrich and anonymize each flow and
store the results in a zipped pickle file 
'''
import datetime

# region ----------------------------------------------------------------- anonymization parameters
PERMUTATION_SEED                     = b'foobar' 
# endregion

# region ----------------------------------------------------------------- elasticsearch parameters
ELASTICSEARCH_HOST                   = 'XXX.XXX.XXX.XXX'
ELASTICSEARCH_PORT                   = 9200
ELASTICSEARCH_SCROLL_SIZE            = 10000
ELASTICSEARCH_SCROLL_CONTEXT_TIMEOUT = '1h'  # m, h, d (nanos, micros, ms, s)
# SEARCH_TIME_INTERVAL_LOW = '0000000000000'  # '0000000000000', '1970-01-01 01:00:00' (time zone UTC)
SEARCH_TIME_INTERVAL_DELTA = '5d'  # s, m, H/h, d, w, M, y

SEARCH_TIME_INTERVAL_HIGH  = '2019-01-31 10:13:00'  # -1 hour
SEARCH_TIME_INTERVAL_HIGH  = datetime.datetime.strptime(SEARCH_TIME_INTERVAL_HIGH, '%Y-%m-%d %H:%M:%S')
SEARCH_TIME_INTERVAL_HIGH  = int(SEARCH_TIME_INTERVAL_HIGH.timestamp()) * 1000 
SEARCH_TIME_INTERVAL_HIGH  = 'now'

SEARCH_TIME_INTERVAL_LOW   = '2019-01-31 10:01:00'  # '1548925200' # '2019-01-31 09:00:00'
SEARCH_TIME_INTERVAL_LOW   = datetime.datetime.strptime(SEARCH_TIME_INTERVAL_LOW, '%Y-%m-%d %H:%M:%S')
SEARCH_TIME_INTERVAL_LOW   = int(SEARCH_TIME_INTERVAL_LOW.timestamp()) * 1000 
SEARCH_TIME_INTERVAL_LOW   = SEARCH_TIME_INTERVAL_HIGH + '-' + SEARCH_TIME_INTERVAL_DELTA

ELASTICSEARCH_INDEX   = 'netflow-*'
ELASTICSEARCH_DOCTYPE = 'netflow'
ELASTICSEARCH_BODY    = {
  "query": {
    "bool": {
      "must_not": [
        {"range":{"netflow.src_addr":{"gte":"224.0.0.0", "lte":"255.255.255.255"}}},
        {"range":{"netflow.dst_addr":{"gte":"224.0.0.0", "lte":"255.255.255.255"}}}
      ],
      "must": [
        # {"range":{"@timestamp":{"gte":SEARCH_TIME_INTERVAL_LOW,
        #                         "lte":SEARCH_TIME_INTERVAL_HIGH}}},
        # {"range":{"netflow.bytes":{"gte":'10000000'}}}        
      ]
    }
  }
}    

FLOW_KEYS = [
	'netflow.first_switched', 'netflow.last_switched',
	'netflow.bytes', 'netflow.protocol',
	'netflow.dst_addr', 'netflow.dst_port',
	'netflow.src_addr', 'netflow.src_port',
	'netflow.src_locality', 'netflow.dst_locality',
	'netflow.tcp_flags', 'netflow.flow_seq_num',
	'host',
	]
#endregion

PERMUTATION_TABLES = None

import utils
import geo
from elasticsearch import Elasticsearch
import numpy as np
import hashlib
from ipaddress import IPv4Address
import prefix_lookup as pl

@utils.measure_time_memory
def init():
  # load geo information
  geo.load_data()
  
  # load prefix information  
  pl.load_prefix_data()
  
  # hash password (permutation seed) based on SHA3-512
  sha3_512_hash = hashlib.sha512(PERMUTATION_SEED).hexdigest()
  
  # create 4 seeds, one for each permutation table
  seeds = [
    int(sha3_512_hash[0:8], 16),
    int(sha3_512_hash[56:64], 16),
    int(sha3_512_hash[64:72], 16),
    int(sha3_512_hash[120:128], 16), 
    ]
  
  # create 4 individual permutation tables for each octet of an ip address 
  global PERMUTATION_TABLES 
  PERMUTATION_TABLES = [ np.random.RandomState(seed=seed).permutation(np.arange(256)) for seed in seeds ]

  
@utils.measure_time_memory
def process_flows():
  ''' load, enrich (update), anonymize (convert) and store flows pagewise '''
  
  elastic = Elasticsearch(hosts=[{'host': ELASTICSEARCH_HOST,
                                  'port': ELASTICSEARCH_PORT}])
  
  number_of_elements = elastic.count(index=ELASTICSEARCH_INDEX,
                                     doc_type=ELASTICSEARCH_DOCTYPE,
                                     body=ELASTICSEARCH_BODY)['count'] 
  
  number_of_pages = number_of_elements // ELASTICSEARCH_SCROLL_SIZE
  print('number_of_elements', number_of_elements)
  print('ELASTICSEARCH_SCROLL_SIZE', ELASTICSEARCH_SCROLL_SIZE)
  
  page = elastic.search(index=ELASTICSEARCH_INDEX,
                        doc_type=ELASTICSEARCH_DOCTYPE,
                        scroll=ELASTICSEARCH_SCROLL_CONTEXT_TIMEOUT,
                        size=ELASTICSEARCH_SCROLL_SIZE,
                        body=ELASTICSEARCH_BODY,
                        _source=FLOW_KEYS)
  
  scroll_id   = page['_scroll_id']
  scroll_size = page['hits']['total']
  
  print('scroll_size_total', scroll_size)
  
  i = 0
  while (scroll_size > 0):
    flows = [ x['_source'] for x in page['hits']['hits'] ]
    flows = [ {k.split('.')[1] if k != 'host' else k:x[k] for k in x.keys()} for x in flows ]
    update_flows(flows)
    convert_flows(flows)
    utils.pickle_flows(flows)
    utils.printProgressBar(i, number_of_pages, prefix='Progress:', suffix='Complete', length=50)
    page        = elastic.scroll(scroll_id=scroll_id, scroll=ELASTICSEARCH_SCROLL_CONTEXT_TIMEOUT)    
    scroll_id   = page['_scroll_id']
    scroll_size = len(page['hits']['hits'])
    i += 1


def update_flows(flows):
  '''
  enrich (update) flows with local and global topology information
  
  @param flows: flows to be enriched (list)
  '''  
  for flow in flows:
    if flow['src_locality'] == 'private':
      flow.update({ 'src_' + k:v for k, v in geo.hsfd_geo_data.items() if k in geo.GEO_KEYS })
      src_prefix, src_vlan = pl.get_prefix_for_ip_private(flow['src_addr'])
    else: 
      flow.update({ 'src_' + k:v for k, v in geo.get_geo_information(flow['src_addr']).items() if k in geo.GEO_KEYS })
      src_prefix, src_vlan = pl.get_prefix_for_ip_public(flow['src_addr'])
      
    if flow['dst_locality'] == 'private': 
      flow.update({ 'dst_' + k:v for k, v in geo.hsfd_geo_data.items() if k in geo.GEO_KEYS })
      dst_prefix, dst_vlan = pl.get_prefix_for_ip_private(flow['dst_addr'])
    else: 
      flow.update({ 'dst_' + k:v for k, v in geo.get_geo_information(flow['dst_addr']).items() if k in geo.GEO_KEYS })
      dst_prefix, dst_vlan = pl.get_prefix_for_ip_public(flow['dst_addr'])

    flow.update({'src_network'   : str(src_prefix.network_address),
                 'src_prefix_len': src_prefix.prefixlen,
                 'src_vlan'      : src_vlan,
                 'dst_network'   : str(dst_prefix.network_address),
                 'dst_prefix_len': dst_prefix.prefixlen,
                 'dst_vlan'      : dst_vlan})


def convert_flows(flows):
  '''
  anonymize (convert) flows
  
  @param flows: flows to be anonymized (list)
  '''
  
  def convert_ips(flow):
    '''
    convert each ip address in a flow (source/destination address/network)
    
    @param flow: flow for which the ip addresses are converted (dict)
    '''

    def permute_ip(ip):
      ''' 
      permute an ip address octet by octet based on individual permutation tables
      
      @param ip: ip address to be permuted (str)
      @return permuted ip address (str)
      '''
      return '.'.join([str(PERMUTATION_TABLES[i][int(octet)]) for i, octet in enumerate(ip.split('.'))])
    
    flow['src_addr']    = permute_ip(flow['src_addr'])
    flow['dst_addr']    = permute_ip(flow['dst_addr'])
    
    flow['src_network'] = permute_ip(flow['src_network'])
    flow['dst_network'] = permute_ip(flow['dst_network'])
  
  for flow in flows:
    convert_ips(flow)
 
  
if __name__ == '__main__':
  print('Anonymizer')

  init()
  process_flows()
  
  print('EXIT')