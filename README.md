# Anonymizer
Dependencies:  Python version: 3.6+ (for used packages see [utils.py](utils.py))

## Description and Usage

The anonymizer module implements the Enrichment and Anonymization steps of the Data Preparation stage.

![Flow Data Pipeline](/images/flow_data_pipeline.png)

First, the flow data is loaded from an elasticsearch database, enriched with additional metadata, anonymized and stored as a pickeled raw dataset named `<timestamp>_flows.pkl.gz`. 
The process can be started with `python3 anonymizer.py`.
The raw dataset file can be further processed by the module [Flow Dataset and DNN](https://gitlab.cs.hs-fulda.de/flow-data-ml/icann19/flow_dataset_and_dnn).

## Parameters

Elasticsearch and other parameters (e.g., for the anonymization) are specified in [main.py](main.py) (e.g., `ELASTICSEARCH_HOST`, `PERMUTATION_SEED`)

## Enrichment 

A global prefixes and ASN database is downloaded automatically and stored in [/db/](/db/).
To enrich local flows with local prefixes and VLANs, add [/db/private_prefixes.csv](/db/private_prefixes.csv) and [/db/private_prefixes_vlans.csv](/db/private_prefixes_vlans.csv) (examples given).

To increase the performace of the database lookup, install the [MaxMind DB Python Module](https://github.com/maxmind/MaxMind-DB-Reader-python) respectively follow the installation instructions from [https://github.com/maxmind/libmaxminddb](https://github.com/maxmind/libmaxminddb)


This solution includes GeoLite2 data created by MaxMind, available from
<a href="https://www.maxmind.com">https://www.maxmind.com</a>.

## Paper Reference
A Study of Deep Learning for Network Traffic Data Forecasting, ICANN 2019