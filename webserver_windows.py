from waitress import serve
from airflow.www.app import create_app

app = create_app()
serve(app, host='0.0.0.0', port=8080)
