# DS 3022 Data Project 2 (Fall 2026): The SQS Puzzle

## **OVERALL**

### **What is this project?** 
This project builds an **end to end Prefect data pipeline** that collects randomly delayed message fragments from an SQS queue and ultimately assembles and submits them as a complete phrase. The pipeline uses DuckDB to store the collected data and dbt to test and transform it before the final submission.


### **How to setup and run?**
* After cloning the repository and configuring the required variables in .env (including UVA_ID, the local SQS endpoint, and the provided DP2 settings), the project runs across **four active components/terminals!**
    * **The local SQS environment:** Start the Moto SQS emulator with `moto_server -p 9324`. This acts like AWS SQS locally and holds the queues and messages that the pipeline sends, receives, and deletes.
    * **The scatter API server:**  Run `python kit/scatter_api.py`. This API creates the SQS queue and populates it with the 21 randomly delayed message fragments after receiving the pipeline’s POST request.
    * **The Prefect server/dashboard:** runs Prefect’s local server so the pipeline’s flow and task states, logs, successes, and failures can be monitored through the Prefect dashboard. Run the commands `prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"` and `prefect server start`
    * **The Prefect flow itself:** This is where the main pipeline flow is actually run, carrying the data from the initial POST request through final submission...


**From the repo-root, the pipeline runs end to end with one command: ``` python prefect-flow.py```**

This is the file that hosts the Prefect flow and its tasks, which perform the following: 

1. POST request to Scatter API
2. Monitoring and collecting the resulting queue messages
3. Landing each fragment in DuckDB before deleting it from the queue
4. Dbt as a quality gate, testing and transforming the data before assembling as an ordered phrase
5. Reads from DuckDB and makes final SQS submission to dp2-submit queue



## **Checkpoint A**

This bit of code populates the SQS queue and monitors the randomly delayed messages until they become available. 

* In **populate_queue()**, the pipeline sends one POST request to SCATTER_URL and returns the sqs_url needed by the rest of the flow. 
* The **get_counts()** helper then uses get_queue_attributes() to retrieve three important queue states: visible, in-flight, and delayed messages.

**An important element: polling strategy**

Because the messages are released after random delays, *checking these counters only once would not be enough.* Instead, **monitor_queue() uses a polling strategy— I chose to have it repeatedly call get_counts(queue_url) inside a while loop** to get an updated view of the queue. 

* This gives the pipeline an updated view of the queue without constantly making unnecessary requests, and it can **continue as soon as c["delayed"] == 0** rather than waiting a predetermined amount of time. 
* I also use time.monotonic() and timeout_s to set a deadline, so the task **raises a TimeoutError** rather than continuing to poll forever if something goes wrong.
* **I rejected checking the queue only once** because some messages could still be delayed, and I also **avoided continuously checking with no pause** because that would create unnecessary requests while the queue had not changed. 
* **A fixed wait time was also less appropriate** because the delays are random: a short wait could continue too early, while a long wait could waste time after all messages were already available.


For Checkpoint A, I have also provided an architecture/DAG diagram with task boundaries and dependencies. The diagram shows the boundaries between each Prefect task, the order in which data moves through the pipeline, and the dependencies between tasks. **The DAG diagram can be found in docs/DAG_diagram.png**



## **Checkpoint B**
* In collect_messages(), each SQS message is **first received with receive_message(), then persisted to DuckDB in raw.fragments, and only afterward deleted from the queue using its ReceiptHandle.**
    * **I chose this order to protect against data loss:** if the flow crashes before the DuckDB insert, the message has not been deleted and can become available to be received again; if it crashes after the insert but before deletion, the message may be received again and create a duplicate raw row rather than being lost completely. 
* **To make this behavior idempotent** (what happens if the flow crashes between steps), stored_count() uses *count(distinct order_no)* so repeated fragments do not increase the number of collected fragments. stg_fragments.sql uses *SELECT DISTINCT* to remove identical duplicate rows before the dbt tests and phrase assembly.
    * This means a retry may repeat work after a crash. But, an identical repeated message does not change the final result.



## **Checkpoint C**
dbt serves as the pipeline’s quality gate, testing the collected fragments before the final phrase can be assembled and submitted. I executed **two dbt tests** where I intentionally modified the raw data to confirm that the quality gate could detect invalid data.

* I removed one of the 21 fragments, causing the sequence test to fail. Evidence: **docs/checkpointC_failedtest1.png**
* I added a conflicting fragment with an existing order_no, causing the uniqueness test to fail. **Evidence: docs/checkpointC_failedtest2.png**
* After restoring valid data, all dbt tests passed successfully. Evidence: **docs/checkpointC_completedtest.png**



## **Checkpoint D**
Checkpoint D connects the completed tasks into a single Prefect flow. The submit_solution() task checks and logs the HTTP status code to confirm a successful submission. The final submission was verified using kit/check_submission.py. 

* **Proof:** 

    RECEIPT: 8F0DF768E0

    Screenshot of this recipt can be found at **docs/checkpointD_checksubmission.png**



## **CHECKPOINT F**
During this checkpoint I tested the pipeline on both a Delayed run (longer message delays) and a Chaos run (poison messages).

**Delayed Run** (DP2_FAST=0, which uses the full 30–900 second message delays.)
The pipeline successfully continued polling as messages became available with longer delays and completed end to end.

* **Proof:** 

   RECEIPT: 8F0DF768E0

   Screenshot of full delayed run can be found at **docs/checkpointF_delayed.png**




**Chaos Round**

* **What broke:** During my first Chaos run, collect_messages() crashed with ```KeyError: 'word'``` as the log line. My original code assumed every message contained both order_no and word, so the malformed (“poison”) message caused the task to fail before the queue could be emptied.

* **Why:** The code directly accessed a["word"]["StringValue"], but the poison message did not contain a word attribute. This showed that the original collector could handle normal messages but not malformed ones.

* **How I fixed it:** I added **parse()** to validate messages before inserting them. Malformed messages now return None, produce a “Rejected malformed message” warning, and are stored in raw.rejects. Also important, the rejected message is still deleted using its ReceiptHandle. If it was simply skipping it would allow it to reappear in the queue and prevent the counters from ever reaching zero.

* **Proof:**
    * **docs/CheckpointF_chaosmode.png** shows two consecutive successful chaos rounds.
    * **docs/checkpointF_chaosproof.png** shows a "Rejected" warning followed by the queue counters reaching zero, demonstrating that both runs succesfully stored 21 fragments. 








## ORIGINAL README

---
This data project is a puzzle. Your goal is to put the pieces back together in the right order.

You will write a **Prefect** data pipeline that retrieves 21 messages from an SQS queue, parses their contents, and reassembles a complete phrase from the fragments, one word per message. Your pipeline can run a single time to produce a complete result, or it can run on a schedule. Design your workflow however you see fit. You can run it as many times as you like while testing.

> Adapted from Neal Magee's Fall 2025 Data Project 2 (UVA). The puzzle is the same. Two things are new this year:
> 1. **The queue runs on your laptop.** A free SQS emulator replaces AWS, so you need no AWS account or credentials. Your code is still ordinary `boto3`: one environment variable (`AWS_ENDPOINT_URL_SQS`) decides whether it talks to your laptop or to real AWS.
> 2. **dbt is the quality gate.** Fragments are landed in DuckDB, and a dbt model reassembles the phrase. dbt tests must pass before your flow is allowed to submit.

**Due:** Monday 19 October 2026, **23:59** (Lesson 11 that afternoon is a working session: evidence run, chaos round and independent build time with instructor help). **Weight:** 20%.

Before anything else, follow **[SETUP.md](SETUP.md)** until `python kit/smoke_test.py` prints `Emulator OK`. SETUP.md also explains the four terminals you keep open while you work.
Work through **[CHECKPOINTS.md](CHECKPOINTS.md)**, check your work against **[RUBRIC.md](RUBRIC.md)**, and keep **[PROGRESS.md](PROGRESS.md)** up to date.

## Architecture

```
POST /api/scatter/<UVA_ID>          (kit/scatter_api.py, your laptop)
        |
        v
SQS queue <UVA_ID>  -- 21 messages, each delayed 30-900 s --
        |
        v
Prefect flow  (prefect-flow.py)
   monitor counters -> receive -> land in DuckDB raw.fragments -> delete
        |
        v
dbt build  (dbt/)
   stg_fragments (typed, de-duplicated) -> tests -> assembled_phrase
        |
        v
send_message to queue dp2-submit  ->  python kit/check_submission.py  ->  receipt code
```

## Task 1: Populate your SQS queue

Make an HTTP `POST` request to the scatter API with your UVA computing ID (the scatter API must be running, see SETUP.md):

```
http://127.0.0.1:8000/api/scatter/<UVA_ID>
```

```python
import requests

url = "http://127.0.0.1:8000/api/scatter/mst3k"
payload = requests.post(url).json()
```

```python
import httpx

url = "http://127.0.0.1:8000/api/scatter/mst3k"
payload = httpx.post(url).json()
```

The response gives you your queue URL:

```
>>> payload
{'hello': 'mst3k', 'sqs_url': 'http://127.0.0.1:9324/123456789012/mst3k', 'messages': 21, 'mode': 'fast (5-60s delays)'}
```

Your request sends exactly **21** messages to your queue, each with a random `DelaySeconds` between **30 and 900 seconds** (5 to 60 seconds while `DP2_FAST=1`, for development). **Keep these delays in mind as your pipeline proceeds to the next task.**

**NOTE:** Do not repeat this POST if your pipeline runs more than once (for example on a schedule) or inside a retry loop. Each POST empties your queue and sends all 21 messages again.

## Task 2: Monitor your queue, then collect, land and delete

Track how many messages are waiting with `get_queue_attributes()`. Three values count messages, and together they make up the total:

- `ApproximateNumberOfMessages` (visible, ready to receive)
- `ApproximateNumberOfMessagesNotVisible` (received by someone, not yet deleted)
- `ApproximateNumberOfMessagesDelayed` (still hidden by their delay)

Decide on a strategy for how and when to pick up messages, explain it in your README, and code it.

Each message has a `Body` containing the same meaningless word. The meaningful content is in `MessageAttributes`. Ask for it explicitly with `MessageAttributeNames=["All"]`, then read each attribute's `StringValue`:

```python
msg = resp["Messages"][0]
order_no = msg["MessageAttributes"]["order_no"]["StringValue"]   # a string, not an int
word     = msg["MessageAttributes"]["word"]["StringValue"]
```

Requirements:

- **Land every fragment in DuckDB** (default file `dp2.duckdb`, table `raw.fragments` with at least `order_no`, `word`, `received_at`) **before** you delete its message.
- Delete each message with its `ReceiptHandle` after storing it. Your pipeline must receive, store and delete **all** messages. Leave no "dangling" messages behind.
- Handle empty or delayed responses gracefully. When nothing is visible, `receive_message` returns a response **without** a `Messages` key; your pipeline must not crash on it.
- Use a timeout so the pipeline fails clearly, rather than hanging forever, if fragments never arrive.

## Task 3: Reassemble with dbt, then submit

Build the phrase with dbt inside `dbt/` (the project, profile and source are already set up):

- `models/staging/stg_fragments.sql`: cast `order_no` to an integer and clean `word`. Make this robust to a fragment being stored twice.
- `models/staging/schema.yml`: generic tests (`not_null`, `unique`, ...) on its columns.
- `tests/`: at least one singular test that fails unless there are exactly 21 fragments with no gaps in the order numbers.
- `models/marts/assembled_phrase.sql`: one row containing the phrase in order.

Your flow runs `dbt build --project-dir dbt --profiles-dir dbt` (or `dbtRunner` from `dbt.cli.main`) and **must not submit if dbt fails**.

Example of ordering:

```
order_no   word
--------   -----
3          brown
2          quick
4          fox.
1          The
```

gives "The quick brown fox."

Submit your phrase as a message to the separate queue **`dp2-submit`**, with your computing ID and the platform used (`"prefect"`) as message attributes:

```python
def send_solution(uvaid, phrase, platform):
    url = sqs.get_queue_url(QueueName="dp2-submit")["QueueUrl"]
    response = sqs.send_message(
        QueueUrl=url,
        MessageBody="dp2 solution",
        MessageAttributes={
            "uvaid":    {"DataType": "String", "StringValue": uvaid},
            "phrase":   {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform},
        },
    )
    ...
```

Check and log that the response has HTTP status `200`. Then run:

```
python kit/check_submission.py
```

and paste the **receipt code** it prints into your README.

You may observe and rerun your pipeline as often as you like, but in the end your code must fetch, store, test, reassemble and submit without human intervention. You may not receive messages or sort fragments by hand.

## Notes and submission

1. Fork the assignment repository and push your work to your fork.
2. Your Prefect flow lives in `prefect-flow.py`. Secondary flows (subflows) are allowed.
3. Use Prefect's built-in logging. Do not commit log files.
4. Do not commit data, `.duckdb` files, `dbt/target/` or `.env` (the provided `.gitignore` handles this).
5. Do not edit the files in `kit/`: they are the same for everyone and are used to check your submission.
6. Your **README must include**: how to set up and run your pipeline; your polling strategy and why; how your dbt tests protect the result (with a screenshot of a test failing on purpose and then passing); a Prefect dashboard screenshot of one **full-delay run** (`DP2_FAST=0`); your receipt code; and a "Chaos round" section (Lesson 11).
7. Submit the repository link and your self-assessed **[RUBRIC.md](RUBRIC.md)**. Your README is where you explain and defend your design (25% of the grade).

## Reference

- [Working with SQS: practical examples (Neal Magee)](https://github.com/nmagee/learn-sqs)
- [boto3 SQS client documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html)
- [Prefect documentation](https://docs.prefect.io/v3/get-started)
- [dbt-duckdb adapter](https://github.com/duckdb/dbt-duckdb) and [dbt data tests](https://docs.getdbt.com/docs/build/data-tests)
- [Moto server mode](https://docs.getmoto.org/en/latest/docs/server_mode.html), [ElasticMQ](https://github.com/softwaremill/elasticmq)
