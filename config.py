import os
from dotenv.main import load_dotenv

load_dotenv()

kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
print("KAFKA_BOOTSTRAP_SERVERS:", kafka_bootstrap_servers)

kafka_topic = os.getenv("KAFKA_TOPIC")
print("KAFKA_TOPIC:", kafka_topic)

kafka_group = os.getenv("KAFKA_GROUP")
print("KAFKA_GROUP:", kafka_group)

csv_sep = os.getenv('CSV_SEP')
print("CSV_SEP:", csv_sep)
