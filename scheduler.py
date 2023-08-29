import requests
import json
import math
import time
import csv
import subprocess
import os
import traceback
from collections import defaultdict
import threading
import pprint

avg_over_time = "30s"
prometheus_address = "localhost:9090"
query = "word_count"
flink_rest_api = "http://localhost:8081"
cool_down = 240
overprovisioning_factor = 1
ops = ['Source:_Source_One', 'Source:_Source_Two', 'FlatMap_tokenizer', 'Count_op', 'Sink:_Dummy_Sink']


def need_reschedule(old_parallelism, new_parallelism):
    for op in new_parallelism:
        if op in old_parallelism and old_parallelism[op] != new_parallelism[op]:
            return True
    return False

#find the current running job
def find_running_job():
    job_id = ''
    job_id_json = requests.get(f"{flink_rest_api}/jobs/")
    for job in job_id_json.json()['jobs']:
        if job['status'] == 'RUNNING':
            job_id = job['id']

    assert job_id != ''
    return job_id

def extract_per_operator_metrics(metrics_json, include_subtask=False):
    metrics = metrics_json.json()["data"]["result"]
    metrics_per_operator = {}
    for operator in metrics:
        if include_subtask:
            metrics_per_operator[operator["metric"]["task_name"] + " " + operator["metric"]["subtask_index"]] = float(operator["value"][1])
        else:
            metrics_per_operator[operator["metric"]["task_name"]] = float(operator["value"][1])
    return metrics_per_operator

def generate_graph(json_data):
    graph = defaultdict(list)

    # Extract the nodes from the job graph
    nodes = json_data["plan"]["nodes"]

    # Iterate over each node
    for node in nodes:
        node_id = node["id"]
        description = node["description"]
        parallelism = node["parallelism"]

        # Extract the node name from the description
        node_name = description.split('<')[0].strip()

        # Initialize the adjacency list for the node
        # graph[node_name] = []

        # Extract the input connections for the node
        if "inputs" in node:
            inputs = node["inputs"]
            # Iterate over each input connection
            for connection in inputs:
                source_node_id = connection["id"]

                # Find the source node in the list of nodes
                source_node = next((n for n in nodes if n["id"] == source_node_id), None)

                if source_node:
                    source_description = source_node["description"]
                    source_node_parallelism = source_node["parallelism"]
                    source_node_name = source_description.split('<')[0].strip()

                    # Add the source node to the adjacency list of the current node
                    graph[(source_node_name, source_node_parallelism)].append((node_name, parallelism))

    return graph

class scheduler:

    def call_ds2(self):
        print("---Calling DS2---")

        command = [
            "cargo",
            "run",
            "--manifest-path",
            "./ds2/controller/Cargo.toml",
            "--release",
            "--bin",
            "policy",
            "--",
            "--topo",
            "ds2_query_data/flink_topology_" + query + ".csv",
            "--rates",
            "ds2_query_data/flink_rates_" + query + ".log",
            "--system",
            "flink"
        ]


        # parse the result of DS2
        ds2_model_result = subprocess.run(command, capture_output=True, text=True)
        # print(ds2_model_result.stderr)
        output_text = ds2_model_result.stdout
        output_err = ds2_model_result.stderr
        print(output_err)
        print(output_text)
        # output_text_values = output_text.split("\n")[-2]
        # output_text_values = output_text_values[1:-1]

        # suggested_parallelism = {}
        #
        # filtered = []
        # for val in output_text_values.split(','):
        #     if "topic" not in val:
        #         val = val.replace(" NodeIndex(0)\"", "")
        #         val = val.replace(" NodeIndex(4)\"", "")
        #         filtered.append(val)
        #
        # for i in range(0, len(filtered), 2):
        #     suggested_parallelism[filtered[i]] = math.ceil(float(filtered[i + 1].replace("\"", ""))*overprovisioning_factor)
        # if suggested_parallelism[filtered[i]] == 0:
        #     raise Exception("parallelism is zero")
        # if suggested_parallelism[filtered[i]] > 16:
        #     suggested_parallelism[filtered[i]] = 16
        # if suggested_parallelism[filtered[i]] <= 0:
        #     suggested_parallelism[filtered[i]] = 1
        #
        # print("---Calling DS2---DONE")
        # print("NEW parallelism: ")
        # print(suggested_parallelism)
        #
        # return suggested_parallelism

    def collect_and_write_data_to_file(self):
        print("---Collecting and Writing Metrics---")

        # a way to make sure that prometheus service is up!
        prometheus_is_up = False
        while not prometheus_is_up:
            input_rate_query = requests.get(
                "http://" + prometheus_address + "/api/v1/query?query=rate(flink_taskmanager_job_task_numRecordsInPerSecond[" + avg_over_time + "])")
            input_rates_per_operator = extract_per_operator_metrics(input_rate_query, include_subtask=True)
            if len(input_rates_per_operator) == 0:
                time.sleep(5)
            else:
                prometheus_is_up = True

        input_rate_query = requests.get(
            "http://" + prometheus_address + "/api/v1/query?query=rate(flink_taskmanager_job_task_numRecordsInPerSecond[" + avg_over_time + "])")
        output_rate_query = requests.get(
            "http://" + prometheus_address + "/api/v1/query?query=rate(flink_taskmanager_job_task_numRecordsOutPerSecond[" + avg_over_time + "])")
        busy_time_query = requests.get(
            "http://" + prometheus_address + "/api/v1/query?query=avg_over_time(flink_taskmanager_job_task_busyTimeMsPerSecond[" + avg_over_time + "])")
        number_of_processors_per_task = requests.get(
            "http://" + prometheus_address + "/api/v1/query?query=count(flink_taskmanager_job_task_operator_numRecordsIn) by (task_name)")
        input_rate_kafka = requests.get(
            "http://" + prometheus_address + "/api/v1/query?query=sum(rate(kafka_server_brokertopicmetrics_messagesin_total[1m])) by (topic)")
        true_processing = requests.get(
            "http://" + prometheus_address + "/api/v1/query?query=avg(flink_taskmanager_job_task_numRecordsOutPerSecond) by (task_name) / (avg(flink_taskmanager_job_task_busyTimeMsPerSecond) by (task_name) / 1000)")
        lag = requests.get(
            "http://" + prometheus_address + "/api/v1/query?query=sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max * flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_assigned_partitions) by (task_name)")

        input_rates_per_operator = extract_per_operator_metrics(input_rate_query, include_subtask=True)
        output_rates_per_operator = extract_per_operator_metrics(output_rate_query, include_subtask=True)
        busy_time_per_operator = extract_per_operator_metrics(busy_time_query, include_subtask=True)

        print(input_rates_per_operator)

        busy_time_per_operator['Source:_Source_One 0'] = 1000
        busy_time_per_operator['Source:_Source_Two 0'] = 1000

        processors_per_operator = extract_per_operator_metrics(number_of_processors_per_task)
        operators = list(processors_per_operator.keys())
        input_rate_kafka = extract_per_operator_metrics(input_rate_kafka)
        lag = extract_per_operator_metrics(lag)

        print("AAAAAA")
        print(input_rates_per_operator)
        print(output_rates_per_operator)
        print(busy_time_per_operator)

        true_processing_rate = {}
        for key in input_rates_per_operator:
            true_processing_rate[key] = input_rates_per_operator[key] / (busy_time_per_operator[key] / 1000)
            print("Operator: " + key + " InputRate: " + str(input_rates_per_operator[key]))

        true_output_rate = {}
        for key in output_rates_per_operator:
            true_output_rate[key] = output_rates_per_operator[key] / (busy_time_per_operator[key] / 1000)
            print("Operator: " + key + " OutputRate: " + str(output_rates_per_operator[key]))

        with open('./ds2_query_data/flink_rates_' + query + '.log', 'w+', newline='') as f:
            writer = csv.writer(f)
            header = ["# operator_id", "operator_instance_id", "total_number_of_operator_instances", "epoch_timestamp", "true_processing_rate", "true_output_rate", "observed_processing_rate", "observed_output_rate"]
            writer.writerow(header)

            timestamp = time.time_ns()
            # for key in input_rate_kafka.keys():
            #     row = [key, 0, 1, timestamp, 1, 1, 1, 1]
            #     writer.writerow(row)

            for key in input_rates_per_operator:
                formatted_key = key.split(" ")
                operator, operator_id = formatted_key[0], formatted_key[1]
                row = [operator, operator_id, int(processors_per_operator[operator]), timestamp, true_processing_rate[key], true_output_rate[key], input_rates_per_operator[key], output_rates_per_operator[key]]
                print(row)
                writer.writerow(row)
        # print("Wrote rates file")

        job_id = find_running_job()
        plan = requests.get(f"{flink_rest_api}/jobs/{job_id}/plan")
        graph = generate_graph(plan.json())

        print("---------------")
        print()
        print(graph)
        print()
        print("---------------")

        with open('./ds2_query_data/flink_topology_' + query + '.csv', 'w+', newline='') as f:
            writer = csv.writer(f)
            header = ["# operator_id1","operator_name1","total_number_of_operator_instances1","operator_id2","operator_name2","total_number_of_operator_instances2"]
            writer.writerow(header)

            for k, v in graph.items():
                for node in v:
                    row = [k[0].replace(' ', '_'), k[0].replace(' ', '_'), k[1], node[0].replace(' ', '_'), node[0].replace(' ', '_'), node[1]]
                    writer.writerow(row)

        print("---Collecting and Writing Metrics---DONE")
        print("OLD parallelism: ")
        print(processors_per_operator)
        return processors_per_operator

    def restart(self, path_to_savepoint, new_parallelism):
        def restart_job():
            print("---Restarting Job---")
            command = [
                'docker', 'exec', '-it', 'jobmanager', './bin/flink', 'run',
                '-s', path_to_savepoint,
                '--class', 'ch.ethz.systems.strymon.ds2.flink.wordcount.TwoInputsWordCount',
                'target_jar',
                '--p1', str(1 if not ops[0] in new_parallelism else new_parallelism[ops[0]]),
                '--p2', str(1 if not ops[1] in new_parallelism else new_parallelism[ops[1]]),
                '--p3', str(1 if not ops[2] in new_parallelism else new_parallelism[ops[2]]),
                '--p4', str(1 if not ops[3] in new_parallelism else new_parallelism[ops[3]]),
                '--p5', str(1 if not ops[4] in new_parallelism else new_parallelism[ops[4]])
            ]
            subprocess.Popen(command)
            print("---Restarting Job DONE---")

        # Create a thread for the restart_job function
        thread = threading.Thread(target=restart_job)

        # Start the thread
        thread.start()

    def stop(self, job_id):
        # stop
        print("---Stopping Job---")
        stop_job_url = f"{flink_rest_api}/jobs/{job_id}/stop"
        stop_request = requests.post(stop_job_url)

        job_status_url = f"{flink_rest_api}/jobs/{job_id}"
        job_running = True

        while job_running:
            time.sleep(1)  # Wait for 1 second before checking the status again
            status_request = requests.get(job_status_url)
            status_data = status_request.json()
            job_status = status_data.get("state", "UNKNOWN")

            if job_status == "RUNNING":
                print("Job is still running...")
            else:
                print(f"Job status: {job_status}")
                job_running = False

        print("---Stopping Job DONE---")

    def take_savepoint(self):
        print("---Taking Savepoint---")
        # get the job id
        job_id = find_running_job()

        # take savepoint
        trigger_savepoint_url = f"{flink_rest_api}/jobs/{job_id}/savepoints"
        print(trigger_savepoint_url)
        savepoint = requests.post(trigger_savepoint_url)

        trigger_id = savepoint.json()['request-id']
        print(f"{flink_rest_api}/jobs/{job_id}/savepoints/{trigger_id}")

        # wait for the savepoint to complete
        savepoint_status_url = f"{flink_rest_api}/jobs/{job_id}/savepoints/{trigger_id}"
        response = requests.get(savepoint_status_url)
        status = response.json()["status"]["id"]
        savepoint_name = requests.get(f"{flink_rest_api}/jobs/{job_id}/savepoints/{trigger_id}")

        while status == "IN_PROGRESS" or "location" not in savepoint_name.json()["operation"]:
            response = requests.get(savepoint_status_url)
            status = response.json()["status"]["id"]

            savepoint_name = requests.get(f"{flink_rest_api}/jobs/{job_id}/savepoints/{trigger_id}")
            time.sleep(10)

        assert status == "COMPLETED"

        print(savepoint_name.json())
        savepoint_path = savepoint_name.json()["operation"]["location"]
        print(savepoint_path)

        print("---Taking Savepoint DONE---")
        return job_id, savepoint_path

    def run(self):
        while True:
            old_parallelism = self.collect_and_write_data_to_file()
            new_parallelism = self.call_ds2()
            # if need_reschedule(old_parallelism, new_parallelism):
            #     job_id, path_to_savepoint = self.take_savepoint()
            #     self.stop(job_id)
            #     self.restart(path_to_savepoint, new_parallelism)
            print("---Sleeping---")
            time.sleep(120)
            print("---Sleeping---DONE")

s = scheduler()
s.run()