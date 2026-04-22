import azure.functions as func
from kaggle.api.kaggle_api_extended import KaggleApi
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime
import os

app = func.FunctionApp()

@app.route(route="run-pipeline", auth_level=func.AuthLevel.FUNCTION)
def run_pipeline(req: func.HttpRequest) -> func.HttpResponse:
    today = datetime.now().strftime("%Y%m%d")

    account_name = "bigdataeducation"
    account_key = os.getenv("ACCOUNT_KEY")
    file_system_name = "dataset"

    local_file_path = "./data/Life Expectancy Data.csv"
    target_path = f"{today}/led_{today}.csv"

    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(
        "kumarajarshi/life-expectancy-who",
        path="./data",
        unzip=True
    )

    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )

    file_system_client = service_client.get_file_system_client(file_system_name)
    file_client = file_system_client.get_file_client(target_path)

    with open(local_file_path, "rb") as f:
        file_client.upload_data(f, overwrite=True)

    return func.HttpResponse("Upload complete", status_code=200)