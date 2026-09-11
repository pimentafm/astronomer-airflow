"""
## Delete a collection in Weaviate

CAUTION: This DAG will delete a specified collection in your Weaviate instance.
Meant to be used during development to reset Weaviate.
Please use it with caution.
"""

from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.sdk import dag, task
import os

# Provider your Weaviate conn_id here.
WEAVIATE_CONN_ID = os.getenv("WEAVIATE_CONN_ID", "weaviate_default")
# Provide the name of the collection to delete.
WEAVIATE_COLLECTION_TO_DELETE = "MY_COLLECTION_TO_DELETE"


@dag(
    dag_display_name="🧼 Delete a Collection in Weaviate",
    schedule=None,
    catchup=False,
    description="CAUTION! Will delete a collection in Weaviate!",
    tags=["helper"]
)
def clear_weaviate():

    @task(
        task_display_name=f"Delete {WEAVIATE_COLLECTION_TO_DELETE} in Weaviate",
    )
    def delete_weaviate_collection(collection_to_delete=None):
        WeaviateHook(WEAVIATE_CONN_ID).delete_collections(
            collection_names=collection_to_delete
        )

    delete_weaviate_collection(collection_to_delete=WEAVIATE_COLLECTION_TO_DELETE)


clear_weaviate()
