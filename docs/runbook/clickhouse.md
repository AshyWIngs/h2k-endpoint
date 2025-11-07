# Подготовка ClickHouse

Документ описывает рабочие процедуры для ClickHouse 24.8.14.39. Инструкции по настройке кластера с отключенной репликацией для чтения из каждой ноды кластера Kafka и DDL по созданию таблиц для чтения данных из топиков Kafka.

## 1. Настройка кластера "per_host_allnodes"

- К существующему кластеру *shardless* добавить конфигурацию кластера *per_host_allnodes* по примеру *QA*
/etc/clickhouse-server/config.d/clickhouse_remote_servers.xml:
```
    <per_host_allnodes>
      <shard>
          <internal_replication>false</internal_replication>
          <replica>
              <host>10.254.3.111</host>
              <port>9000</port>
          </replica>
      </shard>
      <shard>
          <internal_replication>false</internal_replication>
          <replica>
              <host>10.254.3.112</host>
              <port>9000</port>
          </replica>
      </shard>
      <shard>
          <internal_replication>false</internal_replication>
          <replica>
              <host>10.254.3.113</host>
              <port>9000</port>
          </replica>
      </shard>
      <shard>
          <internal_replication>false</internal_replication>
          <replica>
              <host>10.254.3.114</host>
              <port>9000</port>
          </replica>
      </shard>
    </per_host_allnodes>
```

## 2. Общий каталог для всех таблиц из Kafka

```sql
CREATE DATABASE IF NOT EXISTS kafka ON CLUSTER per_host_allnodes;
```

## 3. TBL_JTI_TRACE_CIS_HISTORY

### 3.1 

```sql
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_sink ON CLUSTER per_host_allnodes SYNC;

```