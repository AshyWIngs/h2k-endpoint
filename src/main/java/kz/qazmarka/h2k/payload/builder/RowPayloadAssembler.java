package kz.qazmarka.h2k.payload.builder;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.config.CfFilterSnapshot;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.TableOptionsSnapshot;
import kz.qazmarka.h2k.config.TableValueSource;
import kz.qazmarka.h2k.payload.serializer.avro.AvroValueCoercer;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Формирует Avro {@link GenericData.Record} для строки WAL без промежуточных карт.
 * План обработки таблицы вычисляется один раз и кешируется.
 */
final class RowPayloadAssembler implements AutoCloseable {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(RowPayloadAssembler.class);

    private static final String FIELD_EVENT_TS = PayloadFields.EVENT_TS;
    private static final String FIELD_DELETE = PayloadFields.DELETE;

    private final Decoder decoder;
    private final H2kConfig cfg;
    private final QualifierCache qualifierCache = new QualifierCache();
    private final Set<String> tableOptionsLogged = ConcurrentHashMap.newKeySet();
    private final TableSchemaCache schemaCache;
    private final LinkedHashMap<String, Object> pkBuffer = new LinkedHashMap<>(8);
    private final RowProcessingState rowState = new RowProcessingState();

    RowPayloadAssembler(Decoder decoder, H2kConfig cfg, AvroSchemaRegistry schemaRegistry) {
        this.decoder = Objects.requireNonNull(decoder, "decoder");
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.schemaCache = new TableSchemaCache(cfg, schemaRegistry);
    }

    GenericData.Record assemble(TableName table,
                                java.util.List<Cell> cells,
                                RowKeySlice rowKey,
                                long walSeq,
                                long walWriteTime) {
        if (LOG.isDebugEnabled()) {
            debugTableOptions(table);
        }

        TableSchema schema = schemaCache.schemaFor(table);
        ensureRowKeyPresent(table, rowKey);
        GenericData.Record avroRecord = schema.newRecord();

        decodePrimaryKey(table, schema, rowKey, avroRecord);
        RowProcessingState state = rowState;
        state.reset();
        fillRecordWithCells(table, schema, avroRecord, cells, state);
        applyFlags(schema, avroRecord, state.maxTimestamp, state.hasDelete, walSeq, walWriteTime);
        schema.verifyPrimaryKey(table, avroRecord);
        return avroRecord;
    }

    @Override
    public void close() {
        qualifierCache.cleanupThreadLocal();
    }

    private void ensureRowKeyPresent(TableName table, RowKeySlice rowKey) {
        if (rowKey == null) {
            throw new IllegalStateException(
                    "Отсутствует rowkey для таблицы " + table.getNameWithNamespaceInclAsString());
        }
    }

    private void decodePrimaryKey(TableName table,
                                  TableSchema schema,
                                  RowKeySlice rowKey,
                                  GenericData.Record avroRecord) {
        pkBuffer.clear();
        decoder.decodeRowKey(table, rowKey, schema.saltBytes(), pkBuffer);
        schema.applyPrimaryKey(table, avroRecord, pkBuffer);
    }

    private void fillRecordWithCells(TableName table,
                                     TableSchema schema,
                                     GenericData.Record avroRecord,
                                     java.util.List<Cell> cells,
                                     RowProcessingState state) {
        if (cells != null) {
            for (Cell cell : cells) {
                if (cell != null) {
                    long ts = cell.getTimestamp();
                    if (ts > state.maxTimestamp) {
                        state.maxTimestamp = ts;
                    }
                    if (CellUtil.isDelete(cell)) {
                        state.hasDelete = true;
                    } else {
                        writeCellValue(table, schema, avroRecord, cell);
                    }
                }
            }
        }
    }

    // Вынос обработки ячейки в отдельный метод удерживает цикл простым и понятным.
    private void writeCellValue(TableName table,
                                TableSchema schema,
                                GenericData.Record avroRecord,
                                Cell cell) {
        String qualifier = qualifierCache.intern(
                cell.getQualifierArray(),
                cell.getQualifierOffset(),
                cell.getQualifierLength());
        FieldSpec field = schema.fieldNormalized(qualifier);
        if (field == null || field.skip()) {
            return;
        }

        Object decodedValue = decoder.decode(
                table,
                cell.getQualifierArray(),
                cell.getQualifierOffset(),
                cell.getQualifierLength(),
                cell.getValueArray(),
                cell.getValueOffset(),
                cell.getValueLength());

        Object valueToWrite = decodedValue;
        if (valueToWrite == null && cell.getValueLength() > 0) {
            valueToWrite = BinarySlice.of(
                    cell.getValueArray(),
                    cell.getValueOffset(),
                    cell.getValueLength());
        }

        if (valueToWrite != null) {
            field.write(avroRecord, valueToWrite);
        }
    }

    private void applyFlags(TableSchema schema,
                            GenericData.Record avroRecord,
                            long maxTimestamp,
                            boolean hasDelete,
                            long walSeq,
                            long walWriteTime) {
        if (schema.eventTimestampIndex() >= 0 && maxTimestamp != Long.MIN_VALUE) {
            avroRecord.put(schema.eventTimestampIndex(), maxTimestamp);
        }
        if (schema.deleteFlagIndex() >= 0 && hasDelete) {
            avroRecord.put(schema.deleteFlagIndex(), Boolean.TRUE);
        }
        schema.applyWalMetadata(avroRecord, walSeq, walWriteTime);
    }

    private static final class RowProcessingState {
        long maxTimestamp;
        boolean hasDelete;

        void reset() {
            maxTimestamp = Long.MIN_VALUE;
            hasDelete = false;
        }
    }

    private void debugTableOptions(TableName table) {
        String tableName = table.getNameWithNamespaceInclAsString();
        if (!tableOptionsLogged.add(tableName)) {
            return;
        }
        TableOptionsSnapshot snapshot = cfg.describeTableOptions(table);
        String saltSource = label(snapshot.saltSource());
        String capacitySource = label(snapshot.capacitySource());
        CfFilterSnapshot cfSnapshot = snapshot.cfFilter();
        String cfLabel;
        if (cfSnapshot.enabled()) {
            String csv = cfSnapshot.csv();
            cfLabel = (csv == null || csv.isEmpty()) ? "-" : csv;
        } else {
            cfLabel = "-";
        }
        String cfSource = label(cfSnapshot.source());
        LOG.debug("Таблица {}: соль={} ({}), capacityHint={} ({}), cf={} ({})",
                tableName,
                snapshot.saltBytes(),
                saltSource,
                snapshot.capacityHint(),
                capacitySource,
                cfLabel,
                cfSource);
    }

    private static String label(TableValueSource source) {
        return source == null ? "" : source.label();
    }

    private static final class TableSchemaCache {
        private final H2kConfig cfg;
        private final AvroSchemaRegistry schemaRegistry;
        private final String skipProperty;
        private final ConcurrentHashMap<String, TableSchema> cache = new ConcurrentHashMap<>();

        TableSchemaCache(H2kConfig cfg, AvroSchemaRegistry schemaRegistry) {
            this.cfg = cfg;
            this.schemaRegistry = schemaRegistry;
            this.skipProperty = cfg.getPayloadSkipProperty();
        }

        TableSchema schemaFor(TableName table) {
            String key = table.getNameWithNamespaceInclAsString();
            return cache.computeIfAbsent(key, k -> buildSchema(table));
        }

        private TableSchema buildSchema(TableName table) {
            Schema schema = loadSchema(table);
            Map<String, FieldSpec> fields = buildFieldSpecs(table, schema);
            String[] pkColumns = cfg.primaryKeyColumns(table);
            int[] pkIndices = mapPkIndices(table, pkColumns, fields);

            int eventTsIndex = indexOf(schema, FIELD_EVENT_TS);
            int deleteIndex = indexOf(schema, FIELD_DELETE);
            int walSeqIndex = indexOf(schema, PayloadFields.WAL_SEQ);
            int walWriteTimeIndex = indexOf(schema, PayloadFields.WAL_WRITE_TIME);

            TableOptionsSnapshot options = cfg.describeTableOptions(table);
            int saltBytes = options.saltBytes();
        WalOffsets walOffsets = new WalOffsets(eventTsIndex, deleteIndex, walSeqIndex, walWriteTimeIndex);

        return new TableSchema(schema,
            fields,
            pkIndices,
            saltBytes,
            walOffsets);
        }

        private Schema loadSchema(TableName table) {
            try {
                return schemaRegistry.getByTable(table.getNameAsString());
            } catch (RuntimeException ex) {
                throw new IllegalStateException(
                        "Avro: не удалось загрузить схему для таблицы '" + table.getNameAsString() + "'", ex);
            }
        }

        private Map<String, FieldSpec> buildFieldSpecs(TableName table, Schema schema) {
            Map<String, FieldSpec> fields = new HashMap<>(schema.getFields().size());
            for (Schema.Field field : schema.getFields()) {
                boolean skip = parseSkip(table, field);
                FieldSpec spec = new FieldSpec(field, skip);
                register(fields, field.name(), spec);
            }
            return Collections.unmodifiableMap(fields);
        }

        private void register(Map<String, FieldSpec> fields, String name, FieldSpec spec) {
            fields.put(QualifierCache.normalizeFieldKey(name), spec);
        }

        private boolean parseSkip(TableName table, Schema.Field field) {
            Object raw = resolveSkipRaw(field);
            if (raw == null) {
                return false;
            }
            Object normalized = normalizeSkipValue(raw);
            if (normalized instanceof Boolean) {
                return (Boolean) normalized;
            }
            if (normalized instanceof CharSequence) {
                String text = normalized.toString().trim();
                if (text.isEmpty()) {
                    return false;
                }
                if ("true".equalsIgnoreCase(text)) {
                    return true;
                }
                if ("false".equalsIgnoreCase(text)) {
                    return false;
                }
                warnInvalidSkip(table, field, text);
                return false;
            }
            warnInvalidSkip(table, field, raw);
            return false;
        }

        private Object resolveSkipRaw(Schema.Field field) {
            Object value = field.getObjectProp(skipProperty);
            return value != null ? value : field.getProp(skipProperty);
        }

        private Object normalizeSkipValue(Object raw) {
            if (raw instanceof Boolean || raw instanceof CharSequence) {
                return raw;
            }
            if (raw instanceof com.fasterxml.jackson.databind.JsonNode) {
                com.fasterxml.jackson.databind.JsonNode node = (com.fasterxml.jackson.databind.JsonNode) raw;
                if (node.isBoolean()) {
                    return node.booleanValue();
                }
                if (node.isTextual()) {
                    return node.textValue();
                }
                return null;
            }
            return null;
        }

        private void warnInvalidSkip(TableName table, Schema.Field field, Object raw) {
            LOG.warn("Avro-схема {}: поле {} имеет некорректное значение для {} — будет проигнорировано",
                    table.getNameAsString(), field.name(), skipProperty);
            if (LOG.isDebugEnabled()) {
                LOG.debug("{}: исходное значение='{}'", skipProperty, raw);
            }
        }

        private int[] mapPkIndices(TableName table, String[] pkColumns, Map<String, FieldSpec> fields) {
            if (pkColumns == null || pkColumns.length == 0) {
                return new int[0];
            }
            int[] indices = new int[pkColumns.length];
            for (int i = 0; i < pkColumns.length; i++) {
                String col = pkColumns[i];
                FieldSpec spec = lookupField(fields, col);
                if (spec == null) {
                    throw new IllegalStateException(
                            "Avro: PK колонка '" + col + "' отсутствует в схеме " + table.getNameAsString());
                }
                if (spec.skip()) {
                    throw new IllegalStateException(
                            "Avro: PK колонка '" + col + "' помечена как пропускаемая (" + skipProperty + ")"
                                    + " в схеме " + table.getNameAsString());
                }
                indices[i] = spec.index();
            }
            return indices;
        }

        private FieldSpec lookupField(Map<String, FieldSpec> fields, String name) {
            if (name == null) {
                return null;
            }
            return fields.get(QualifierCache.normalizeFieldKey(name));
        }

        private int indexOf(Schema schema, String name) {
            if (name == null) {
                return -1;
            }
            Schema.Field field = schema.getField(name);
            return field == null ? -1 : field.pos();
        }
    }

    private static final class WalOffsets {
        private final int eventTimestampIndex;
        private final int deleteFlagIndex;
        private final int walSequenceIndex;
        private final int walWriteTimeIndex;

        WalOffsets(int eventTimestampIndex, int deleteFlagIndex, int walSequenceIndex, int walWriteTimeIndex) {
            this.eventTimestampIndex = eventTimestampIndex;
            this.deleteFlagIndex = deleteFlagIndex;
            this.walSequenceIndex = walSequenceIndex;
            this.walWriteTimeIndex = walWriteTimeIndex;
        }
    }

    private static final class TableSchema {
        private final Schema schema;
        private final Map<String, FieldSpec> fields;
        private final int[] pkIndices;
        private final int saltBytes;
        private final WalOffsets walOffsets;

        TableSchema(Schema schema,
                    Map<String, FieldSpec> fields,
                    int[] pkIndices,
                    int saltBytes,
                    WalOffsets walOffsets) {
            this.schema = schema;
            this.fields = fields;
            this.pkIndices = pkIndices;
            this.saltBytes = saltBytes;
            this.walOffsets = walOffsets;
        }

        GenericData.Record newRecord() {
            GenericData.Record avroRecord = new GenericData.Record(schema);
            if (walOffsets.deleteFlagIndex >= 0) {
                avroRecord.put(walOffsets.deleteFlagIndex, Boolean.FALSE);
            }
            return avroRecord;
        }

        FieldSpec field(String qualifier) {
            if (qualifier == null) {
                return null;
            }
            return fields.get(QualifierCache.normalizeFieldKey(qualifier));
        }

        FieldSpec fieldNormalized(String qualifier) {
            if (qualifier == null) {
                return null;
            }
            return fields.get(qualifier);
        }

        int saltBytes() {
            return saltBytes;
        }

        int eventTimestampIndex() {
            return walOffsets.eventTimestampIndex;
        }

        int deleteFlagIndex() {
            return walOffsets.deleteFlagIndex;
        }

        void applyWalMetadata(GenericData.Record avroRecord, long walSeq, long walWriteTime) {
            if (walOffsets.walSequenceIndex >= 0) {
                avroRecord.put(walOffsets.walSequenceIndex, walSeq);
            }
            if (walOffsets.walWriteTimeIndex >= 0) {
                avroRecord.put(walOffsets.walWriteTimeIndex, walWriteTime);
            }
        }

        void applyPrimaryKey(TableName table,
                              GenericData.Record avroRecord,
                              Map<String, Object> pkValues) {
            if (pkValues == null || pkValues.isEmpty()) {
                return;
            }
            for (Map.Entry<String, Object> entry : pkValues.entrySet()) {
                FieldSpec spec = field(entry.getKey());
                if (spec == null) {
                    throw new IllegalStateException(
                            "PK '" + entry.getKey() + "' отсутствует в Avro схеме таблицы "
                                    + table.getNameWithNamespaceInclAsString());
                }
                Object value = entry.getValue();
                if (value == null) {
                    avroRecord.put(spec.index(), null);
                } else {
                    spec.write(avroRecord, value);
                }
            }
        }

        void verifyPrimaryKey(TableName table, GenericData.Record avroRecord) {
            for (int idx : pkIndices) {
                if (avroRecord.get(idx) == null) {
                    String fieldName = schema.getFields().get(idx).name();
                    throw new IllegalStateException(
                            "PK '" + fieldName + "' не восстановлен из rowkey таблицы "
                                    + table.getNameWithNamespaceInclAsString());
                }
            }
        }
    }

    private static final class FieldSpec {
        private final Schema.Field field;
        private final int index;
        private final boolean skip;

        FieldSpec(Schema.Field field, boolean skip) {
            this.field = field;
            this.index = field.pos();
            this.skip = skip;
        }

        boolean skip() {
            return skip;
        }

        int index() {
            return index;
        }

        void write(GenericData.Record avroRecord, Object value) {
            if (skip) {
                return;
            }
            Object coerced = AvroValueCoercer.coerceValue(field.schema(), value, field.name());
            avroRecord.put(index, coerced);
        }
    }

    /**
     * Тестовый адаптер для проверки потокобезопасности интернирования qualifier.
     * Возвращается только из методов {@code *ForTest()} и не должен использоваться в продуктивном коде.
     */
    static final class QualifierCacheProbe {
        private final QualifierCache delegate = new QualifierCache();

        String intern(byte[] array, int offset, int length) {
            return delegate.intern(array, offset, length);
        }
    }

    static QualifierCacheProbe qualifierCacheProbeForTest() {
        return new QualifierCacheProbe();
    }
}
