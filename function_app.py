import azure.functions as func

app = func.FunctionApp()

@app.route(route="run-pipeline", auth_level=func.AuthLevel.ANONYMOUS)
def run_pipeline(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("WORKING")