# CSV 数据写入 Kafka
## 1. 如何安装
安装依赖
```sh
pip install -r requirements.txt
```
修改配置文件
```sh
cat > .env << EOF                                                                                                                                                                                                              ─╯
KAFKA_BOOTSTRAP_SERVERS=192.168.87.7:9092,192.168.87.13:9092,192.168.87.153:9092
KAFKA_TOPIC=test
EOF
```

## 如何使用
``` sh
python app.py
```
