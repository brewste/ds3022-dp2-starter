"""DS 3022 Data Project 2: Prefect flow.

Your pipeline must, without human intervention:
  1. POST to the scatter API to populate your queue (once per run, never inside a retry loop)
  2. Monitor the queue (get_queue_attributes) and collect all 21 messages
  3. Land each fragment in DuckDB (raw.fragments) BEFORE deleting its message
  4. Run dbt: tests are the quality gate, the mart model holds the phrase
  5. Submit the phrase to the 'dp2-submit' queue and log the HTTP status code

Run:   python prefect-flow.py
Then:  python kit/check_submission.py

Every task already has a header. Checkpoint A (Lesson 9): fill in populate_queue,
get_counts and monitor_queue. Lesson 10: collect_messages, dbt_build, read_phrase
and submit_solution. The tasks not called by the flow yet cannot fail, so you can
run the file at any point. The skeleton is a suggestion: rename, split or add
tasks as your design needs, and keep your DAG sketch in step with them.
Use Prefect's logger (get_run_logger) for logging. Never hard-code your
computing ID or the endpoint: read them from the environment (.env).
"""
import os
import time

import boto3
import duckdb                       
from dbt.cli.main import dbtRunner  
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

load_dotenv()

UVA_ID = os.environ["UVA_ID"]
SCATTER_URL = f"http://127.0.0.1:{os.environ.get('SCATTER_PORT', '8000')}/api/scatter/{UVA_ID}"
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")

sqs = boto3.client("sqs")  # the endpoint comes from AWS_ENDPOINT_URL_SQS


@task() #no retry loop for populate_queue
def populate_queue() -> str:
    """POST to the scatter API and return your queue URL."""
    logger = get_run_logger()
    resp = requests.post(SCATTER_URL, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    logger.info(f"Scatter API answered: %s", payload)
    return payload["sqs_url"]


COUNTERS = ["ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed"]

def get_counts(queue_url: str) -> dict:
    """Plain helper (not a task): return the three ApproximateNumberOf... counters as integers."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=COUNTERS)["Attributes"]
    return {"visible":   int(attrs["ApproximateNumberOfMessages"]),
            "in_flight": int(attrs["ApproximateNumberOfMessagesNotVisible"]),
            "delayed":   int(attrs["ApproximateNumberOfMessagesDelayed"])}


@task
def monitor_queue(queue_url: str, poll_s: int = 5,
                  timeout_s: int = 1200) -> dict:
    """Log the three counters every poll_s seconds until nothing is delayed; raise after timeout_s."""
    logger = get_run_logger()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        c = get_counts(queue_url)
        logger.info("visible=%d  in_flight=%d  delayed=%d",
                    c["visible"], c["in_flight"], c["delayed"])
        if c["delayed"] == 0:
            logger.info("All messages released")
            return c
        time.sleep(poll_s)
    raise TimeoutError(f"Messages still delayed after {timeout_s} s")


@task
def prepare_landing() -> None:
    with duckdb.connect(DUCKDB_PATH) as con:
        con.execute("create schema if not exists raw")

        con.execute("""
            create or replace table raw.fragments (
                order_no varchar,
                word varchar,
                received_at timestamp
            ) """)
        
        con.execute("create or replace table raw.rejects ("
                    "message_id varchar, attributes varchar, "
                    "received_at timestamp)")


def parse(msg) -> tuple[str, str] | None:
    a = msg.get("MessageAttributes", {})
    try:
        order_no = a["order_no"]["StringValue"]
        word = a["word"]["StringValue"]
    except KeyError:
        return None                   
    if not order_no.isdigit():
        return None                      
    return order_no, word


def stored_count(con) -> int:
    return con.execute("select count(distinct order_no) "
                       "from raw.fragments").fetchone()[0]


@task  
def collect_messages(queue_url: str, expected: int = 21,
                     timeout_s: int = 1200) -> int:
    """Receive, land in DuckDB, then delete. Return how many fragments are stored."""
   

    logger = get_run_logger()
    
    #Use a timeout so the pipeline fails clearly, rather than hanging forever
    deadline = time.monotonic() + timeout_s 

    with duckdb.connect(DUCKDB_PATH) as con:
        while time.monotonic() < deadline:
            stored, c = stored_count(con), get_counts(queue_url)
            logger.info("stored %d/%d | visible=%d in_flight=%d delayed=%d",
                        stored, expected, c["visible"],
                        c["in_flight"], c["delayed"])
            if stored >= expected and sum(c.values()) == 0:
                return stored
            resp = sqs.receive_message(QueueUrl=queue_url,
                                       MaxNumberOfMessages=10,
                                       MessageAttributeNames=["All"],
                                       WaitTimeSeconds=10)
            
            #Handle empty/delayed responses without crashing
            for msg in resp.get("Messages", []):
                fragment = parse(msg)
                if fragment is None:              # poison message
                    logger.warning("Rejected malformed message %s",
                                   msg["MessageId"])
                    con.execute("insert into raw.rejects "
                                "values (?, ?, current_timestamp)",
                                [msg["MessageId"],
                                 str(msg.get("MessageAttributes"))])
                else:
                    con.execute("insert into raw.fragments "
                                "values (?, ?, current_timestamp)",
                                list(fragment))
                        
                #Delete each message using its ReceiptHandle after storing it
                sqs.delete_message(QueueUrl=queue_url,
                                    ReceiptHandle=msg["ReceiptHandle"])
        
        stored = stored_count(con)
    
    raise TimeoutError(f"Only {stored}/{expected} fragments "
                       f"after {timeout_s} s")




@task
def dbt_build() -> None:
    """Run `dbt build --project-dir dbt --profiles-dir dbt`; raise if anything fails."""
    
    
    res = dbtRunner().invoke(["build", "--project-dir", "dbt",
                              "--profiles-dir", "dbt"])
    if not res.success:
        raise RuntimeError("dbt tests failed: not submitting")
    get_run_logger().info("dbt build passed: all tests green")




@task
def read_phrase() -> str:
    with duckdb.connect(DUCKDB_PATH) as con:
        n, phrase = con.execute("select fragment_count, phrase "
                                "from assembled_phrase").fetchone()
    get_run_logger().info("Assembled %d fragments: %s", n, phrase)
    return phrase




@task(retries=2, retry_delay_seconds=5)
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    url = sqs.get_queue_url(QueueName="dp2-submit")["QueueUrl"]
    resp = sqs.send_message(
        QueueUrl=url, MessageBody="dp2 solution",
        MessageAttributes={
            "uvaid":    {"DataType": "String", "StringValue": UVA_ID},
            "phrase":   {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform}})
    status = resp["ResponseMetadata"]["HTTPStatusCode"]
    get_run_logger().info("Submitted, HTTP %s", status)
    if status != 200:
        raise RuntimeError(f"Submission returned HTTP {status}")
    return status


@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline():
    queue_url = populate_queue()
    prepare_landing()
    monitor_queue(queue_url)
    collect_messages(queue_url)  # ← fills it with 21 rows

    dbt_build()


    phrase = read_phrase()
    submit_solution(phrase)
    



if __name__ == "__main__":
    dp2_pipeline()
