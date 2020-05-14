package com.commerce.commons.constant;


/**
 * 常量
 */
public interface PropertiesConstants {
    String KAFKA_BROKERS = "kafka.brokers";
    String DEFAULT_KAFKA_BROKERS = "localhost:9092";
    String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
    String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";
    String KAFKA_GROUP_ID = "kafka.group.id";
    String DEFAULT_KAFKA_GROUP_ID = "zhisheng";
    String METRICS_TOPIC = "metrics.topic";
    String CONSUMER_FROM_TIME = "consumer.from.time";
    String STREAM_PARALLELISM = "stream.parallelism";
    String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";
    String STREAM_DEFAULT_PARALLELISM = "stream.default.parallelism";
    String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    String STREAM_CHECKPOINT_DIR = "stream.checkpoint.dir";
    String STREAM_CHECKPOINT_TYPE = "stream.checkpoint.type";
    String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    String PROPERTIES_FILE_NAME = "/application.properties";
    String CHECKPOINT_MEMORY = "memory";
    String CHECKPOINT_FS = "fs";
    String CHECKPOINT_ROCKETSDB = "rocksdb";

    //es config
    String ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS = "elasticsearch.bulk.flush.max.actions";
    String ELASTICSEARCH_HOSTS = "elasticsearch.hosts";

    //mysql
    String MYSQL_DATABASE = "mysql.database";
    String MYSQL_HOST = "mysql.host";
    String MYSQL_PASSWORD = "mysql.password";
    String MYSQL_PORT = "mysql.port";
    String MYSQL_USERNAME = "mysql.username";

    //redis
    String LARUNDA_INPUT_FEED_KEY = "larunda.input.feed.key";

    //influxDB
    String INFLUXDB_URL = "influxdb.url";
    String INFLUXDB_USERNAME = "influxdb.username";
    String INFLUXDB_PASSWORD = "influxdb.password";
    String INFLUXDB_DATABASE = "influxdb.database";
    String INFLUXDB_BATCHACTIONS = "influxdb.batchActions";
    String INFLUXDB_FLUSHDURATION = "influxdb.flushDuration";
    String INFLUXDB_ENABLEGZIP = "influxdb.enableGzip";
    String INFLUXDB_CREATEDATABASE = "influxdb.createDatabase";
    String INFLUXDB_MEASUREMENT = "influxdb.measurement";


    //用量统计相关
    Long LONG_36 = 3600000L;
    String TIME_00 = "00:00";
    String TIME_10 = "10:00";
    String TIME_20 = "20:00";
    String TIME_30 = "30:00";
    String TIME_40 = "40:00";
    String TIME_50 = "50:00";

    //Hbase配置
    String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    String HBASE_CLIENT_RETRIES_NUMBER = "hbase.client.retries.number";
    String HBASE_MASTER_INFO_PORT = "hbase.master.info.port";
    String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "hbase.zookeeper.property.clientPort";
    String HBASE_RPC_TIMEOUT = "hbase.rpc.timeout";
    String HBASE_CLIENT_OPERATION_TIMEOUT = "hbase.client.operation.timeout";
    String HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD = "hbase.client.scanner.timeout.period";
    String HBASE_TABLE_NAME = "hbase.table.name";
    String HBASE_COLUMN_NAME = "hbase.column.name";


    //redis 统计用量使用
    String START_TIME = "startTime";
    String END_TIME = "endTime";
}

