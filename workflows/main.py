import os
import dotenv
import requests
import json

def list_job_names():
    return [i.replace(".json", "") for i in os.listdir(".") if i.endswith(".json")]

def load_settings(job_name):
    with open(f"{job_name}.json", "r") as open_file:
        settings = json.load(open_file)
    return settings

def reset_job(settings):
    url = f"https://{DATABRICKS_HOST}/api/2.1/jobs/reset"
    header = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

    resp = requests.post(url=url, headers=header, json=settings)
    return resp

def main():
    for i in list_job_names():
        settings = load_settings(job_name=i)
        resp = reset_job(settings=settings)
        if resp.status_code == 200:
            print(f"Job '{i}' refreshed")
        else:
            print(f"Job '{i}' was not refreshed. Error: {resp.text}")

if __name__ == "__main__":
    main()
