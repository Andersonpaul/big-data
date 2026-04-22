import azure.functions as func
import logging
from kaggle.api.kaggle_api_extended import KaggleApi
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime
import os

app = func.FunctionApp()

@app.route(route="run-pipeline", auth_level=func.AuthLevel.ANONYMOUS)
def run_pipeline(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Pipeline started")

    try:
        # -------------------------
        # 1. DATE
        # -------------------------
        today = datetime.now().strftime("%Y%m%d")
        logging.info(f"Date generated: {today}")

        # -------------------------
        # 2. ENV VARS
        # -------------------------
        account_name = "bigdataeducation"
        account_key = os.getenv("ACCOUNT_KEY")
        logging.info("🔐 Loaded storage account key")

        kaggle_user = os.getenv("KAGGLE_USERNAME")
        kaggle_key = os.getenv("KAGGLE_KEY")

        logging.info(f"Kaggle user loaded: {bool(kaggle_user)}")
        logging.info(f"Kaggle key loaded: {bool(kaggle_key)}")

        if not account_key:
            raise Exception("ACCOUNT_KEY is missing")

        if not kaggle_user or not kaggle_key:
            raise Exception("KAGGLE credentials missing")

        # -------------------------
        # 3. KAGGLE AUTH
        # -------------------------
        os.environ["KAGGLE_USERNAME"] = kaggle_user
        os.environ["KAGGLE_KEY"] = kaggle_key

        logging.info("Kaggle environment variables set")

        api = KaggleApi()
        api.authenticate()
        logging.info("Kaggle authenticated successfully")

        # -------------------------
        # 4. DOWNLOAD DATASET
        # -------------------------
        logging.info("Starting Kaggle dataset download")

        api.dataset_download_files(
            "kumarajarshi/life-expectancy-who",
            path="./data",
            unzip=True
        )

        logging.info("Dataset downloaded successfully")

        # -------------------------
        # 5. FILE CHECK
        # -------------------------
        local_file_path = "./data/Life Expectancy Data.csv"
        logging.info(f"Checking file: {local_file_path}")

        if not os.path.exists(local_file_path):
            raise Exception(f"File not found: {local_file_path}")

        # -------------------------
        # 6. DATA LAKE CONNECTION
        # -------------------------
        logging.info("Connecting to Data Lake")

        service_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=account_key
        )

        file_system_name = "dataset"
        file_system_client = service_client.get_file_system_client(file_system_name)

        logging.info("Connected to file system")

        # -------------------------
        # 7. UPLOAD
        # -------------------------
        target_path = f"{today}/led_{today}.csv"
        logging.info(f"⬆️ Uploading to: {target_path}")

        file_client = file_system_client.get_file_client(target_path)

        with open(local_file_path, "rb") as f:
            file_client.upload_data(f, overwrite=True)

        logging.info("Upload successful")

        return func.HttpResponse("Upload complete", status_code=200)

    except Exception as e:
        logging.exception("Pipeline failed")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)