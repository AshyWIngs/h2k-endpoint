package kz.qazmarka.h2k.endpoint.processing;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Отвечает за построение payload и публикацию строк в Kafka.
 * Держит только необходимые зависимости и не занимается фильтрацией или метриками.
 */
final class WalRowDispatcher {

    private final PayloadBuilder payloadBuilder;
    private final Producer<RowKeySlice, byte[]> producer;

    WalRowDispatcher(PayloadBuilder payloadBuilder, Producer<RowKeySlice, byte[]> producer) {
        this.payloadBuilder = payloadBuilder;
        this.producer = producer;
    }

    void dispatch(String topic,
                  TableName table,
                  WalMeta walMeta,
                  RowKeySlice rowKey,
                  List<Cell> cells,
                  BatchSender sender)
            throws InterruptedException, ExecutionException, TimeoutException {
        byte[] valueBytes = payloadBuilder.buildRowPayloadBytes(table, cells, rowKey, walMeta.seq, walMeta.writeTime);
        sender.add(producer.send(new ProducerRecord<>(topic, rowKey, valueBytes)));
    }
}
