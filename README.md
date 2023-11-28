# Data Lineage using dlt and dbt

This is a demo project that showcases how the dlt load_info can used to create a data lineage overview for your ingested data. We work with the example of a skate shop that runs an online shop using Shopify, in addition to its physical stores. The data from both sources is extracted using dlt and loaded into BigQuery.

![Data Lineage Overview](https://d1ice69yfovmhk.cloudfront.net/images/data_lineage_overview.jpeg)

## Setup Guide
1. Create a `secrets.toml` file in the `.dlt`.
2. Add the service account credentials for BigQuery in the `secrets.toml` file.
    ```
    [credentials]
    client_email = <client_email from services.json>
    private_key = <private_key from services.json>
    project_id = <project_id from services json>
    ```

3. Run the pipeline.
    ```
    python data_lineage.py
    ```
