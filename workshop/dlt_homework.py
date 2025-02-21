import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator



@dlt.resource(name="rides", write_disposition="replace")
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page
# your code is here


pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="bigquery",
    dataset_name="ny_taxi_data"
)



load_info = pipeline.run(ny_taxi, write_disposition="replace")
print(load_info)

