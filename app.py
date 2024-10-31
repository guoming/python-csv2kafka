from datetime import datetime

import pandas as pd
import json
from kafka import KafkaProducer

import config


# 转换时间格式
def convert_time_format(time_str):
    # 将时间从 'YYYY-MM-DD HH:MM:SS' 转换成 'MM/DD/YYYY HH:MM:SS'
    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").strftime("%m/%d/%Y %H:%M:%S")


# 配置 Kafka 生产者
producer = KafkaProducer(
    bootstrap_servers=config.kafka_bootstrap_servers.split(','),  # 替换为 Kafka Broker 的地址
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # 将数据序列化为 JSON 格式
)

# 读取 CSV 文件并转换为 JSON 格式
csv_file_path = 'db/input.csv'  # 替换为你的 CSV 文件路径
df = pd.read_csv(csv_file_path,sep=config.csv_sep)


#df['can_bill_date_time'] = df['can_bill_date_time'].apply(convert_time_format)
#df['order_create_time'] = df['order_create_time'].apply(convert_time_format)

json_records = df.to_dict(orient='records')  # 将 DataFrame 转换为 JSON 格式

# 发送每条记录到 Kafka
topic_name = config.kafka_topic
row_index=0
row_count= len(json_records)

for record in json_records:
    producer.send(topic_name, value=record)

    row_index=row_index+1
    # 输出进度条，\r 回到行首，end='' 防止自动换行
    print(f"\rProgress: {row_index}/{row_count}", end='')

# 刷新并关闭生产者
producer.flush()
producer.close()

print("所有数据已成功发送到 Kafka！")
