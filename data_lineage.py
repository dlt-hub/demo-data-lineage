from dlt.common.runners import Venv

import pandas as pd 
import dlt 
import os 

FILEPATH = "data/supermarket_sales.csv"
FILEPATH_SHOPIFY = "data/orders_export_1.csv"

class Data_Pipeline:

    def __init__(self, pipeline_name, destination, dataset_name):
        self.pipeline_name = pipeline_name
        self.destination = destination
        self.dataset_name = dataset_name

    def run_pipeline(self, data, table_name, write_disposition):
        # Configure the pipeline with your destination details
        pipeline = dlt.pipeline(
            pipeline_name=self.pipeline_name, destination=self.destination, dataset_name=self.dataset_name
        )

        # Run the pipeline with the provided data
        load_info = pipeline.run(data, table_name=table_name, write_disposition=write_disposition)

        # Pretty print the information on data that was loaded
        print(load_info)
        return load_info


if __name__ == "__main__":

    data_store = pd.read_csv(FILEPATH)
    data_shopify = pd.read_csv(FILEPATH_SHOPIFY)

    #filtering some data. 
    select_c_data_store = data_store.loc[:, data_store.columns.difference(['Branch'])]
    select_c_data_shopify = data_shopify.loc[:, data_shopify.columns.difference(['Tags'])]

    select_c_data_store_dict = select_c_data_store.to_dict(orient='records')
    select_c_data_shopify_dict = select_c_data_shopify.to_dict(orient='records')

    data_store_dict = data_store.to_dict(orient='records')
    data_shopify_dict = data_shopify.to_dict(orient='records')

    pipeline_store = Data_Pipeline(pipeline_name='pipeline_store', destination='bigquery', dataset_name='sales_store')
    pipeline_shopify = Data_Pipeline(pipeline_name='pipeline_shopify', destination='bigquery', dataset_name='sales_shopify')

    load_a = pipeline_store.run_pipeline(data=data_store_dict, table_name='sales_info', write_disposition='replace')
    load_b = pipeline_shopify.run_pipeline(data=data_shopify_dict, table_name='sales_info', write_disposition='replace')

    # load_a = pipeline_store.run_pipeline(data=select_c_data_store_dict, table_name='sales_info', write_disposition='replace')
    # load_b = pipeline_shopify.run_pipeline(data=select_c_data_shopify_dict, table_name='sales_info', write_disposition='replace')

    pipeline_store.run_pipeline(data=load_a.load_packages, table_name="load_info", write_disposition="append")
    pipeline_shopify.run_pipeline(data=load_b.load_packages, table_name='load_info', write_disposition="append")


    pipeline_transform = dlt.pipeline(pipeline_name='pipeline_transform', destination='bigquery', dataset_name='sales_transform')

    venv = Venv.restore_current()
    here = os.path.dirname(os.path.realpath(__file__))

    dbt = dlt.dbt.package(
        pipeline_transform, 
        os.path.join(here, "sales_dbt/"),
        venv=venv
        )

    models = dbt.run_all()

    for m in models:
            print(f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}")