Welcome to our example dbt project for using Grafana for Business Intelligence!

### Overview

Data source:
- Our data is coming from BigQuery's bigquery-plubic-data
- In particular, we are going to be visualizing the new_york_citibike dataset

Visualization:
- At Grafana Labs we use Grafana as our visualization layer
- This example will show how 
    1. dbt models can facilitate Grafana panel data
    2. We can connect Grafana to our tables using the new (beta) [Google Bigquery Plugin](https://grafana.com/grafana/plugins/grafana-bigquery-datasource/)
    