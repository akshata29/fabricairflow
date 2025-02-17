import io
import os
import json
import base64
import requests
from PIL import Image
from pendulum import datetime

from airflow.models import Variable
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential


@dag(
    start_date=datetime(2023, 11, 1),
    schedule=None,
    catchup=False,
    params={
        "system_prompt": Param(
            'You are an AI assistant that helps to write an Apache Airflow DAG code by understanding an image that shows an Apache Airflow DAG containing airflow tasks, task descriptions, parameters, trigger rules and edge labels.\
            You have to priortize the Apache Airflow provider operators over Apache Airflow core operators if they resonates more with task description.\
            Use the most appropriate Apache Airflow operators as per the task description\
            To give the label to the DAG edge use the Label from the airflow.utils.edgemodifier class\
            You can use Dummy operators for start and end tasks. \
            Return apache airflow dag code in a valid json format following the format:```json{ "dag": "value should be Apache Airflow DAG code"}```',
            type="string",
            title="Give a prompt to the Airflow Expert.",
            description="Enter what you would like to ask Apache Airflow Expert.",
            min_length=1,
            max_length=500,
        ),
        "seed": Param(42, type="integer"),
        "temperature": Param(0.1, type="number"),
        "top_p": Param(0.95, type="number"),
        "max_tokens": Param(800, type="integer"),
    },
)

def OpenAI_Dag_Generator():
    """
    A DAG that generates an Apache Airflow DAG code using `gpt-4o` OpenAI model based on a diagram image
    stored in Azure Blob Storage. The generated DAG is saved in the `dags` folder for execution.
    """

    @task
    def fetch_image_from_lakehouse(workspace_name: str, file_path: str):
        """
        Downloads an image from Fabric Lakehouse and encodes it as a Base64 string.

        :param workspace_name: Name of the workspace where your Lakehouse is located.
        :param file_path: Relative file path stored in the Fabric Lakehouse.
        :return: Dictionary containing the encoded image as a Base64 string.
        """
        account_url = f"https://onelake.dfs.fabric.microsoft.com"

        client_id = os.getenv("FABRIC_CLIENT_ID")
        client_secret = os.getenv("FABRIC_CLIENT_SECRET")
        tenant_id = os.getenv("FABRIC_TENANT_ID")

        tokenCredential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        lakehouse_client = DataLakeServiceClient(
            account_url,
            credential=tokenCredential
        )

        blob_data = lakehouse_client.get_file_client(workspace_name, file_path).download_file().readall()

        image = Image.open(io.BytesIO(blob_data))

        # Encode image as Base64
        buffered = io.BytesIO()
        image.save(buffered, format="PNG")
        encoded_image = base64.b64encode(buffered.getvalue()).decode('ascii')

        return {"encoded_image": encoded_image}


    @task
    def generate_dag_code_from_openai(image_from_blob: dict, system_prompt: str, **context):
        """
        Sends the encoded image to the OpenAI gpt-4o model to generate an Apache Airflow DAG code.

        :param encoded_image: Dictionary containing the Base64-encoded image.
        :param system_prompt: Prompt to ask the OpenAI model to generate the DAG code.
        :return: Dictionary containing the generated DAG code as a string.
        """

        azureAI_api_key = os.getenv("OPENAI_API_KEY")
        azureAI_endpoint = os.getenv("OPENAI_API_ENDPOINT")

        image = image_from_blob["encoded_image"]

        headers = {
            "Content-Type": "application/json",
            "api-key": azureAI_api_key,
        }

        payload = {
            "messages": [
            {
                "role": "system",
                "content": [
                {
                    "type": "text",
                    "text": system_prompt
                }
                ]
            },
            {
                "role": "user",
                "content": [
                {
                    "type": "image_url",
                    "image_url": {
                    "url": f"data:image/jpeg;base64,{image}"
                    }
                }
                ]
            }
            ],
            "seed": context["params"]["seed"],
            "temperature": context["params"]["temperature"],
            "top_p": context["params"]["top_p"],
            "max_tokens": context["params"]["max_tokens"]
        }

        response = requests.post(azureAI_endpoint, headers=headers, json=payload)
        response.raise_for_status()  

        # Get JSON from request and show
        response_json = response.json()


        # Check if 'choices' and 'message' are present in the response
        if 'choices' in response_json and len(response_json['choices']) > 0:
            content = response_json['choices'][0]['message']['content']

            start_index = content.find('```json')
            end_index = content.rfind("```")

            # Validate JSON block delimiters
            if start_index == -1 or end_index == -1:
                raise ValueError("JSON block delimiters (```json ... ```) not found in the content.")

            # Extract and parse the JSON string
            extracted_json_str = content[start_index + 7:end_index].strip()
            if not extracted_json_str:
                raise ValueError("Extracted JSON string is empty.")

            # Convert to a Python dictionary
            dag_json = json.loads(extracted_json_str)
            dag_code = dag_json.get("dag")
            if not dag_code:
                raise ValueError("'dag' key not found in the extracted JSON.")

            return {"dag_code": dag_code}

        return response_json

    @task
    def save_dag(xcom_dag_code: dict):
        """
        Saves the generated DAG code to a Python file in the `dags` directory.
        """
        try:
            with open("dags/openai_dag.py", "w") as f:
                f.write(xcom_dag_code["dag_code"])

            print("DAG code saved successfully.")
        except Exception as e:
            raise ValueError(f"Error saving DAG code: {str(e)}")

    chain(
        save_dag(
            generate_dag_code_from_openai(
                fetch_image_from_lakehouse(
                    workspace_name="astexternaleus", # Your Fabric Workspace
                    file_path="astexternal.Lakehouse/Files/airflow-dag-diagram.png" # Path to the image file located in the Lakehouse
                ),
                "{{ params.system_prompt }}"
            )
        )
    )

OpenAI_Dag_Generator()
