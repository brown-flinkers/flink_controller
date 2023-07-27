from confluent_kafka import Producer
import random
import string

redpanda_broker = "localhost:19092"
topic = "my_topic"  # Replace with the name of your Redpanda topic

def generate_word(skew_percent=0):
    if skew_percent > 0 and random.randint(1, 100) <= skew_percent:
        return "skew "
    else:
        word_length = random.randint(1, 10)
        return ''.join(random.choice(string.ascii_lowercase) for _ in range(word_length)) + " "

def generate_data():
    desired_sentence_size = 100
    skew_percent = 20  # 20% skew words
    sentence = ""
    while len(sentence) < desired_sentence_size:
        word = generate_word(skew_percent)
        sentence += word
    return sentence.strip()

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def main():
    producer_conf = {
        "bootstrap.servers": redpanda_broker
    }

    producer = Producer(producer_conf)

    try:
        while True:
            data = generate_data()
            message_value = data.encode("utf-8")
            producer.produce(topic, value=message_value, callback=delivery_report)
            producer.poll(0)
    except KeyboardInterrupt:
        print("Data generation stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
