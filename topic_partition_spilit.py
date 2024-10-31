import json
from kafka import KafkaConsumer, KafkaProducer

import config

partition_count = 16  # topic_b 的分区数量
current_partition = 0


# 配置 Kafka 生产者
producer = KafkaProducer(
    bootstrap_servers=config.kafka_bootstrap_servers.split(','),  # 替换为 Kafka Broker 的地址
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # 将数据序列化为 JSON 格式
)
# 创建 Kafka 消费者
kakfa_consumer = KafkaConsumer(
    config.kafka_topic,
    bootstrap_servers=config.kafka_bootstrap_servers,
    group_id=config.kafka_group,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
)

for message in kakfa_consumer:

    # 获取消息数据并写入到另一个 topic
    data = message.value.decode('utf-8')
    producer.send(f'{config.kafka_topic}_v2', value=json.loads(data), partition=current_partition)

    current_partition = (current_partition + 1) % partition_count  # 轮询到下一个分区

    print(f"{data} to topic={config.kafka_topic}_v2 , partition={current_partition}")

    kakfa_consumer.commit()

kakfa_consumer.close()

