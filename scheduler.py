import requests
import json
import math
import time
import csv
import subprocess
import os
import traceback

avg_over_time = "5m"
prometheus_address = "localhost:9090"
query = "word_count"
flink_rest_api = "http://localhost:8081"

def extract_per_operator_metrics(metrics_json, include_subtask=False):
    metrics = metrics_json.json()["data"]["result"]
    metrics_per_operator = {}
    for operator in metrics:
        if include_subtask:
            metrics_per_operator[operator["metric"]["task_name"] + " " + operator["metric"]["subtask_index"]] = float(operator["value"][1])
        else:
            metrics_per_operator[operator["metric"]["task_name"]] = float(operator["value"][1])
    return metrics_per_operator



class scheduler:

    def restart(self, path_to_savepoint):
        command = [
            'docker', 'exec', '-it', 'jobmanager', './bin/flink', 'run',
            '-s', path_to_savepoint,
            '--class', 'ch.ethz.systems.strymon.ds2.flink.wordcount.TwoInputsWordCount',
            'target_jar',
            '--p1', str(2),
            '--p2', str(2),
            '--p3', str(2)
        ]
        subprocess.run(command)

    def take_savepoint(self):
        # get the job id
        job_id_json = requests.get(f"{flink_rest_api}/jobs/")
        job_id = job_id_json.json()['jobs'][0]['id']
        print(job_id)

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

        while status == "IN_PROGRESS":
            response = requests.get(savepoint_status_url)
            status = response.json()["status"]["id"]
            time.sleep(10)

        assert status == "COMPLETED"

        savepoint_name = requests.get(f"{flink_rest_api}/jobs/{job_id}/savepoints/{trigger_id}")
        print(savepoint_name.json())
        savepoint_path = savepoint_name.json()["operation"]["location"]
        print(savepoint_path)

        # stop
        stop_job_url = f"{flink_rest_api}/jobs/{job_id}/stop"
        stop_request = requests.post(stop_job_url)
        print(stop_request)

        return savepoint_path

    def start(self):
        while True:
            input_rate_query = requests.get(
                "http://" + prometheus_address + "/api/v1/query?query=rate(flink_taskmanager_job_task_operator_numRecordsIn[" + avg_over_time + "])")
            output_rate_query = requests.get(
                "http://" + prometheus_address + "/api/v1/query?query=avg_over_time(flink_taskmanager_job_task_numRecordsOutPerSecond[" + avg_over_time + "])")
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

            # previous_scaling_event = requests.get(
            #     "http://" + prometheus_address + "/api/v1/query?query=deriv(flink_jobmanager_numRegisteredTaskManagers[" + cooldown + "])")
            # previous_scaling_event = previous_scaling_event.json()["data"]["result"][0]["value"][1]
            # print("taskmanager deriv: " + str(previous_scaling_event))

            input_rates_per_operator = extract_per_operator_metrics(input_rate_query, include_subtask=True)
            output_rates_per_operator = extract_per_operator_metrics(output_rate_query, include_subtask=True)
            busy_time_per_operator = extract_per_operator_metrics(busy_time_query, include_subtask=True)
            processors_per_operator = extract_per_operator_metrics(number_of_processors_per_task)
            operators = list(processors_per_operator.keys())
            input_rate_kafka = extract_per_operator_metrics(input_rate_kafka)
            lag = extract_per_operator_metrics(lag)
            source_true_processing = extract_per_operator_metrics(true_processing)

            print("Obtained metrics")
            print(operators)

            true_processing_rate = {}
            for key in input_rates_per_operator:
                true_processing_rate[key] = input_rates_per_operator[key] / (busy_time_per_operator[key] / 1000)

            true_output_rate = {}
            for key in output_rates_per_operator:
                true_output_rate[key] = output_rates_per_operator[key] / (busy_time_per_operator[key] / 1000)


            with open('./ds2_query_data/flink_rates_' + query + '.log', 'w+', newline='') as f:
                writer = csv.writer(f)
                header = ["# operator_id", "operator_instance_id", "total_number_of_operator_instances", "epoch_timestamp", "true_processing_rate", "true_output_rate", "observed_processing_rate", "observed_output_rate"]
                writer.writerow(header)

                timestamp = time.time_ns()
                for key in input_rate_kafka.keys():
                    row = [key, 0, 1, timestamp, 1, 1, 1, 1]
                    writer.writerow(row)

                for key in input_rates_per_operator:
                    formatted_key = key.split(" ")
                    operator, operator_id = formatted_key[0], formatted_key[1]
                    row = [operator, operator_id, int(processors_per_operator[operator]), timestamp, true_processing_rate[key], true_output_rate[key], input_rates_per_operator[key], output_rates_per_operator[key]]
                    writer.writerow(row)
            print("Wrote rates file")

            operator_set = set()
            topology_order = []
            edges = {}
            with open('./ds2_query_data/flink_topology_' + query + '.csv', 'w+', newline='') as csvfile:
                reader = csv.reader(csvfile, delimiter='\n')
                for index, row in enumerate(reader):
                    if index == 0:
                        continue
                    row_values = row[0].split(",")
                    if row_values[0] not in edges:
                        edges[row_values[0]] = [row_values[3]]
                    else:
                        edges[row_values[0]].append(row_values[3])
                    # if row_values[0] not in operator_set:
                    #     topology_order.append(row_values[0])
                    #     operator_set.add(row_values[0])
                    # if row_values[3] not in operator_set:
                    #     topology_order.append(row_values[3])
                    #     operator_set.add(row_values[3])
            print(edges)

            lag_per_topic = {}
            source_to_topic={"Source:_BidsSource":"bids_topic", "Source:_auctionsSource":"auction_topic", "Source:_personSource":"person_topic", "Source:_BidsSource____Timestamps_Watermarks":"bids_topic"}
            for key, value in lag.items():
                lag_per_topic[source_to_topic[key]] = float(value) / lag_processing_time

            print("source rate")
            print(input_rate_kafka)
            print("extra rate due to lag")
            print(lag_per_topic)

            with open("ds2_query_data/" + query + "_source_rates.csv", 'w+', newline='') as f:
                writer = csv.writer(f)
                header = ["# source_operator_name","output_rate_per_instance (records/s)"]
                writer.writerow(header)
                for key, value in input_rate_kafka.items():
                    row = [key, input_rate_kafka[key]]
                    writer.writerow(row)
            print("Wrote source rate file")

            # setting number of operators for kafka topic to 1 so it can be used in topology.
            for key in input_rate_kafka.keys():
                processors_per_operator[key] = 1

            with open('./ds2_query_data/flink_topology_' + query + '2.csv', 'w+', newline='') as f:
                writer = csv.writer(f)
                header = ["# operator_id1","operator_name1","total_number_of_operator_instances1","operator_id2","operator_name2","total_number_of_operator_instances2"]
                writer.writerow(header)
                for key in edges.keys():
                    for edge in edges[key]:
                        row = [key, key, int(processors_per_operator[key]),edge, edge,int(processors_per_operator[edge])]
                        writer.writerow(row)
            print("Wrote topology file")

s = scheduler()
savepoint_path = s.take_savepoint()
s.restart(savepoint_path)

