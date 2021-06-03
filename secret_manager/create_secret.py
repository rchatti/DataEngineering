#from google.cloud import secretmanager
from google.cloud import secretmanager_v1 as secretmanager
import hashlib

def create_secret(secret_id):
    """
    Initialize: secretmanager.SecretManagerServiceClient()

    Input Parameters: 
        Secret_ID: Unique Secret Name
        Secret: Secret Settings
        Parent: String built with Location of Project 

    Returns: 
        Response: Use Response.name to print the name  

    """

    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/secret-manager-qstart"
    secret = {"replication": {"automatic": {}}}

    response = client.create_secret(secret_id=secret_id, parent=parent, secret=secret)

    print("Created secret, {}".format(response.name))


def add_secret_version(secret_id, payload):
    """
    Client: secretmanager.SecretManagerServiceClient()

    Inputs: 
        Secret_ID: STR in the format, projects/{project_id}/secrets/{secret_id}
        payload: DICT in the format, {'data':payload}

    Output:
        response.name: "Added Secret version projects/933414730734/secrets/My_Second_Secret/versions/1"
    """

    client = secretmanager.SecretManagerServiceClient()


    parent = "projects/secret-manager-qstart/secrets/{}".format(secret_id)
    print(parent)

    payload = payload.encode('UTF-8')

    response = client.add_secret_version(parent = parent, payload={'data':payload})

    print("Added Secret version {}".format(response.name))


def access_secret(project_id, secret_id, version="latest"):
        client = secretmanager.SecretManagerServiceClient()
        parent = "projects/{}/secrets/{}/versions/{}".format(project_id, secret_id, version)
        response = client.access_secret_version(name=parent)
        payload = response.payload.data.decode('UTF-8')

        print("Secret payload in {} is \"{}\"".format(response.name, payload))

access_secret('933414730734', 'My_Second_Secret')
add_secret_version("My_Second_Secret", "Payload version 2")

access_secret('933414730734', 'My_Second_Secret',1)
access_secret('933414730734', 'My_Second_Secret')