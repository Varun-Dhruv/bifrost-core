import kinto_http

KINTO_SERVER_URL = "http://localhost:8888/v1"
client = kinto_http.Client(server_url=KINTO_SERVER_URL, auth=("user", "pass"))


def add_config(config):
    try:
        bucket = client.get_bucket("bifrost")
        if not bucket:
            client.create_bucket("config")
        client.get_collection(bucket="config")
        client.create_record(bucket="config", collection="config", data=config)
    except kinto_http.exceptions.KintoException:
        print("Error adding config to Kinto")
