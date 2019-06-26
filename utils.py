'''
load/install modules
helper functions (load csv file, pickle data)
'''


def load_module(module, package=None):
  ''' 
  auto module loader, if module not present, install into user space via pip  
  
  @param module: module name 
  @param package: (optional) package name to install if different from module name
  '''  
  try:
    import pip
    if hasattr(pip, 'main'): from pip           import main as pip_install
    else                   : from pip._internal import main as pip_install
  except: 
    raise Exception('install pip')
  try:
    import importlib
    importlib.import_module(module)
  except ImportError:
    if package is None: pip_install(['install', '--user', module])  
    else              : pip_install(['install', '--user', package]) 
  finally:
    globals()[module] = importlib.import_module(module)


# required modules
modules = [
  'geoip2',
  'elasticsearch',
  'numpy',
  'hashlib',
  'psutil',
  'urllib',
  'pickle',
  'gzip',
  ]

# auto install routine for modules
for module in modules: load_module(module)

import psutil
import time
import os
import gzip
import pickle
from datetime import datetime

PICKLE_FILE_FLOWS    = None
PICKLE_FILE_PREFIXES = 'public_prefixes.pkl.gz'


def measure_time_memory(method):
  '''
  decorator for time and memory measurement of functions
  
  @param method: function for which the measurements are performed (func)
  @return result(s) of the execution of method
  '''
  def measure(*args, **kw):
    print('{}'.format(method.__name__), end='', flush=True)
    start  = time.time()
    result = method(*args, **kw)
    if hasattr(psutil.Process(), 'memory_info'):
      mem = psutil.Process(os.getpid()).memory_info()[0] // (2 ** 20)
      print('...{:2.2f}s mem: {}MB'.format(time.time() - start, mem))
    elif hasattr(psutil.Process(), 'memory_full_info'):
      mem = psutil.Process(os.getpid()).memory_full_info()[0] // (2 ** 20)
      print('...{:2.2f}s mem: {}MB'.format(time.time() - start, mem))
    else:
      print('...{:2.2f}s '.format(time.time() - start))
    return result

  return measure


def load_csv_file(filename, _filter=None, _select=None, skip_header=False):
  '''
  load a csv file
  
  @param filename   : local csv filename/path to the csv file (str)
  @param _filter    : filter function that is applied to each line of the csv file (func)
  @param _select    : select function that is applied to each line of the csv file (func)
  @param skip_header: whether the first line should be skipped or not (bool)
  @return lines of the csv file (list of str)
  '''
  data = []
  with open(filename, encoding='utf8') as csv_file:
    if skip_header: next(csv_file)
    
    for line in csv_file:
      if not line.strip() or line.startswith('#'): continue
      if _filter is not None and _filter(line): 
        continue 
      if _select is not None:
        line = _select(line)
      data.append(line.strip())
  return data


def load_pickle_file(filename):
  '''
  load a pickle file
  
  @param filename: local pickle filename/path to the pickle file (str)
  @return unpickled data (list)
  '''  
  if not filename.endswith('.pkl.gz'): filename += '.pkl.gz'
  
  print('load pickle file {}'.format(filename), end='', flush=True)
  
  if os.path.dirname(os.path.realpath(__file__)) not in filename:
    folder_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
  else:
    folder_filename = filename
  if not os.path.isfile(folder_filename): assert('pickle file {} not found'.format(filename))
  
  print('...size: {} MB\n'.format(os.path.getsize(folder_filename) // 2 ** 20))
  
  # if the file contain multiple objects, append all to a list
  with gzip.open(folder_filename, 'rb') as file:
    result = []
    while True:
      try:
        result += pickle.load(file)
      except:
        return result

      
def pickle_data(data, filename, pickle_file):
  '''
  pickle data to file
  
  @param data: picklable data
  @param filename: filename of the pickle file (str)
  @param pickle_file: filename of/path to an existing pickle file (str/None)
  '''     
  folder_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
   
  # if file already exists, append  
  with gzip.GzipFile(folder_filename, 'wb' if pickle_file is None else 'ab') as file:
    pickle.dump(data, file, pickle.HIGHEST_PROTOCOL)      


def pickle_flows(flows):
  '''
  pickle flows
  
  @param flows: flows (list of dict)
  '''  
  global PICKLE_FILE_FLOWS
   
  if PICKLE_FILE_FLOWS is None:
    filename = '{}_flows.pkl.gz'.format(datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S'))
    print('...file {}'.format(filename), end='', flush=True)    
  else:
    filename = PICKLE_FILE_FLOWS
     
  folder_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
   
  pickle_data(flows, folder_filename, PICKLE_FILE_FLOWS)
   
  PICKLE_FILE_FLOWS = filename

  
def pickle_prefixes(prefixes):
  '''
  pickle prefix lookup tree
  
  @param prefixes: prefix lookup tree (prefix_lookup_public, prefix_lookup_private)
  '''
  folder_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), PICKLE_FILE_PREFIXES)
  pickle_data(prefixes, folder_filename, PICKLE_FILE_PREFIXES)

  
def printProgressBar (iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█'):
  ''' print a progress bar '''
  if total == 0: total = 1
  percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
  filledLength = int(length * iteration // total)
  bar = fill * filledLength + '-' * (length - filledLength)
  print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
  if iteration == total: 
    print()
