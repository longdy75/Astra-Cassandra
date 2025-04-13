 # Dylan Long - 4/12/25
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

# Setup session

SECURE_CONNECT_BUNDLE = "/secure-connect-sales-100.zip"

def get_cassandra_session():
        
    try:
        with open("Sales_100-token.json") as f:
            secrets = json.load(f)

            CLIENT_ID = secrets["clientId"]
            CLIENT_SECRET = secrets["secret"]

            auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
            cluster = Cluster(cloud={'secure_connect_bundle' : SECURE_CONNECT_BUNDLE},auth_provider=auth_provider)
            session = cluster.connect()
            print("Successfully connected to Astra DB.")

            return session

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
