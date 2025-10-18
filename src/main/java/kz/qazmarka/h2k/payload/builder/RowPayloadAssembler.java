package kz.qazmarka.h2k.payload.builder;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Табличный план (поля, индексы PK, служебные поля) вычисляется один раз и переиспользуется.
 */
final class RowPayloadAssembler {

    private static final Logger LOG = LoggerFactory.getLogger(RowPayloadAssembler.class);
    private static final String FIELD_EVENT_TS = PayloadFields.EVENT_TS;
    private static final String FIELD_DELETE = PayloadFields.DELETE;

    private final Decoder decoder;
    private final H2kConfig cfg;
    private final QualifierCache qualifierCache = new QualifierCache();
    private final Set<String> tableOptionsLogged = ConcurrentHashMap.newKeySet();
    private final TablePlanCache tablePlans;
    private static final CellActionResolver CELL_ACTIONS = new CellActionResolver();

    RowPayloadAssembler(Decoder decoder, H2kConfig cfg, AvroSchemaRegistry schemaRegistry) {
        this.decoder = Objects.requireNonNull(decoder, "decoder");
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.tablePlans = new TablePlanCache(cfg, schemaRegistry);
    }

    GenericData.Record assemble(TableName table,
                                List<Cell> cells,
                                RowKeySlice rowKey,
                                long walSeq,
                                long walWriteTime) {
        if (LOG.isDebugEnabled()) {
            debugTableOptions(table);
        }

        TablePlan plan = tablePlans.planFor(table);
        GenericData.Record avroRecord = plan.newRecord();

        PkCollector pkCollector = new PkCollector(plan);
        pkCollector.bind(avroRecord);
        decodePrimaryKey(table, rowKey, plan, pkCollector);

        RowAssemblyState state = new RowAssemblyState(table, plan, avroRecord);
        if (cells != null) {
            for (Cell cell : cells) {
                if (cell == null) {
                    continue;
                }
                state.accept(cell);
            }
        }
        state.applyFlags();

        plan.applyWalMetadata(avroRecord, walSeq, walWriteTime);

        return avroRecord;
    }

    /**
     * Состояние сборки строки: аккумулирует метаданные и делегирует обработку ячеек стратегиям.
     */
    private final class RowAssemblyState {
        private final TableName table;
        private final TablePlan plan;
        private final GenericData.Record avroRecord;
        private long maxTimestamp = Long.MIN_VALUE;
        private boolean hasDelete;

        RowAssemblyState(TableName table, TablePlan plan, GenericData.Record avroRecord) {
            this.table = table;
            this.plan = plan;
            this.avroRecord = avroRecord;
        }

        void accept(Cell cell) {
            long ts = cell.getTimestamp();
            if (ts > maxTimestamp) {
                maxTimestamp = ts;
            }
            CELL_ACTIONS.resolve(cell).apply(this, cell);
        }

        void handleDelete(Cell cell) {
            Objects.requireNonNull(cell, "cell");
            hasDelete = true;
        }

        void handleValue(Cell cell) {
            String column = qualifierCache.intern(
                    cell.getQualifierArray(),
                    cell.getQualifierOffset(),
                    cell.getQualifierLength());
            FieldPlan field = plan.field(column);
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

        void applyFlags() {
            if (plan.eventTsIndex >= 0 && maxTimestamp != Long.MIN_VALUE) {
                avroRecord.put(plan.eventTsIndex, maxTimestamp);
            }
            if (plan.deleteIndex >= 0 && hasDelete) {
                avroRecord.put(plan.deleteIndex, Boolean.TRUE);
            }
        }
    }

    private enum CellAction {
        UPSERT {
            @Override
            void apply(RowAssemblyState state, Cell cell) {
                state.handleValue(cell);
            }
        },
        DELETE {
            @Override
            void apply(RowAssemblyState state, Cell cell) {
                state.handleDelete(cell);
            }
        };

        abstract void apply(RowAssemblyState state, Cell cell);
    }

    /**
     * Быстрая таблица действий для типов ячеек WAL, чтобы избежать повторного ветвления в горячем цикле.
     */
    private static final class CellActionResolver {
        private final CellAction[] lookup;

        CellActionResolver() {
            this.lookup = initLookup();
        }

        CellAction resolve(Cell cell) {
            byte type = cell.getTypeByte();
            return lookup[type & 0xFF];
        }

        private static CellAction[] initLookup() {
            CellAction[] arr = new CellAction[256];
            Arrays.fill(arr, CellAction.UPSERT);
            markDelete(arr, org.apache.hadoop.hbase.KeyValue.Type.Delete);
            markDelete(arr, org.apache.hadoop.hbase.KeyValue.Type.DeleteColumn);
            markDelete(arr, org.apache.hadoop.hbase.KeyValue.Type.DeleteFamily);
            markDelete(arr, org.apache.hadoop.hbase.KeyValue.Type.DeleteFamilyVersion);
            return arr;
        }

        private static void markDelete(CellAction[] arr, org.apache.hadoop.hbase.KeyValue.Type type) {
            arr[type.getCode() & 0xFF] = CellAction.DELETE;
        }
    }

    private void decodePrimaryKey(TableName table,
                                  RowKeySlice rowKey,
                                  TablePlan plan,
                                  PkCollector collector) {
        if (rowKey == null) {
            throw new IllegalStateException(
                    "Отсутствует rowkey для таблицы " + table.getNameWithNamespaceInclAsString());
        }
        Map<String, Object> buffer = collector.target();
        decoder.decodeRowKey(table, rowKey, plan.saltBytes, buffer);
        collector.applyToRecord(table);
        plan.verifyPk(table, collector.currentRecord());
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

    /**
     * Кеш табличных планов (Avro-схема, индексы полей, PK, служебные поля).
     */
    private static final class TablePlanCache {
        private final H2kConfig cfg;
        private final AvroSchemaRegistry schemaRegistry;
        private final String skipProperty;
        private final ConcurrentHashMap<String, TablePlan> cache = new ConcurrentHashMap<>();

        TablePlanCache(H2kConfig cfg, AvroSchemaRegistry schemaRegistry) {
            this.cfg = cfg;
            this.schemaRegistry = schemaRegistry;
            this.skipProperty = cfg.getPayloadSkipProperty();
        }

        TablePlan planFor(TableName table) {
            String key = table.getNameWithNamespaceInclAsString();
            return cache.computeIfAbsent(key, k -> buildPlan(table));
        }

        private TablePlan buildPlan(TableName table) {
            Schema schema = loadSchema(table);
            Map<String, FieldPlan> fields = buildFieldPlans(table, schema);

            String[] pkColumns = cfg.primaryKeyColumns(table);
            int[] pkIndices = new int[pkColumns.length];
            for (int i = 0; i < pkColumns.length; i++) {
                String col = pkColumns[i];
                FieldPlan plan = lookupField(fields, col);
                if (plan == null) {
                    throw new IllegalStateException(
                            "Avro: PK колонка '" + col + "' отсутствует в схеме " + table.getNameAsString());
                }
        if (plan.skip()) {
            throw new IllegalStateException(
                "Avro: PK колонка '" + col + "' помечена как пропускаемая (" + skipProperty + ")"
                    + " в схеме " + table.getNameAsString());
                }
                pkIndices[i] = plan.index;
            }

            int eventTsIndex = indexOfField(schema, FIELD_EVENT_TS);
            int deleteIndex = indexOfField(schema, FIELD_DELETE);

            TableOptionsSnapshot opts = cfg.describeTableOptions(table);
            int saltBytes = opts.saltBytes();

            WalMetadataPlan walMetadata = WalMetadataPlan.of(
                    lookupField(fields, PayloadFields.WAL_SEQ),
                    lookupField(fields, PayloadFields.WAL_WRITE_TIME));
            return new TablePlan(schema,
                    fields,
                    pkIndices,
                    eventTsIndex,
                    deleteIndex,
                    saltBytes,
                    walMetadata);
        }

        private Schema loadSchema(TableName table) {
            try {
                return schemaRegistry.getByTable(table.getNameAsString());
            } catch (RuntimeException ex) {
                throw new IllegalStateException(
                        "Avro: не удалось загрузить схему для таблицы '" + table.getNameAsString() + "'", ex);
            }
        }

        private Map<String, FieldPlan> buildFieldPlans(TableName table, Schema schema) {
            Map<String, FieldPlan> map = new HashMap<>(schema.getFields().size() * 2);
            for (Schema.Field field : schema.getFields()) {
                FieldPlan plan = new FieldPlan(field, parseSkip(table, field));
                map.put(field.name(), plan);
                map.put(field.name().toUpperCase(Locale.ROOT), plan);
            }
            return map;
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

        private FieldPlan lookupField(Map<String, FieldPlan> fields, String name) {
            FieldPlan plan = fields.get(name);
            if (plan != null) return plan;
            if (name == null) return null;
            return fields.get(name.toUpperCase(Locale.ROOT));
        }

        private int indexOfField(Schema schema, String name) {
            if (name == null) {
                return -1;
            }
            for (Schema.Field field : schema.getFields()) {
                if (name.equals(field.name())) {
                    return field.pos();
                }
            }
            return -1;
        }
    }

    private static final class TablePlan {
        private final Schema schema;
        private final Map<String, FieldPlan> fields;
        private final int[] pkFieldIndices;
        private final int eventTsIndex;
        private final int deleteIndex;
        private final int saltBytes;
        private final WalMetadataPlan walMetadata;

        TablePlan(Schema schema,
                  Map<String, FieldPlan> fields,
                  int[] pkFieldIndices,
                  int eventTsIndex,
                  int deleteIndex,
                  int saltBytes,
                  WalMetadataPlan walMetadata) {
            this.schema = schema;
            this.fields = fields;
            this.pkFieldIndices = pkFieldIndices;
            this.eventTsIndex = eventTsIndex;
            this.deleteIndex = deleteIndex;
            this.saltBytes = saltBytes;
            this.walMetadata = walMetadata;
        }

        GenericData.Record newRecord() {
            GenericData.Record avroRecord = new GenericData.Record(schema);
            if (deleteIndex >= 0) {
                avroRecord.put(deleteIndex, Boolean.FALSE);
            }
            return avroRecord;
        }

        FieldPlan field(String name) {
            if (name == null) {
                return null;
            }
            return fields.get(name);
        }

        void verifyPk(TableName table, GenericData.Record avroRecord) {
            for (int idx : pkFieldIndices) {
                if (avroRecord.get(idx) == null) {
                    String fieldName = schema.getFields().get(idx).name();
                    throw new IllegalStateException(
                            "PK '" + fieldName + "' не восстановлен из rowkey таблицы " +
                                    table.getNameWithNamespaceInclAsString());
                }
            }
        }

        void applyWalMetadata(GenericData.Record avroRecord, long walSeq, long walWriteTime) {
            walMetadata.apply(avroRecord, walSeq, walWriteTime);
        }
    }
    private static final class WalMetadataPlan {
        private static final WalMetadataPlan EMPTY = new WalMetadataPlan(null, null);

        private final FieldPlan walSeqField;
        private final FieldPlan walWriteTimeField;

        private WalMetadataPlan(FieldPlan walSeqField, FieldPlan walWriteTimeField) {
            this.walSeqField = walSeqField;
            this.walWriteTimeField = walWriteTimeField;
        }

        static WalMetadataPlan of(FieldPlan walSeqField, FieldPlan walWriteTimeField) {
            if (walSeqField == null && walWriteTimeField == null) {
                return EMPTY;
            }
            return new WalMetadataPlan(walSeqField, walWriteTimeField);
        }

        void apply(GenericData.Record avroRecord, long walSeq, long walWriteTime) {
            if (walSeqField != null) {
                walSeqField.write(avroRecord, walSeq);
            }
            if (walWriteTimeField != null) {
                walWriteTimeField.write(avroRecord, walWriteTime);
            }
        }
    }

    private static final class FieldPlan {
        final Schema.Field field;
        final int index;
        final boolean skip;

        FieldPlan(Schema.Field field, boolean skip) {
            this.field = field;
            this.index = field.pos();
            this.skip = skip;
        }

        void write(GenericData.Record avroRecord, Object value) {
            if (skip) {
                return;
            }
            Object coerced = AvroValueCoercer.coerceValue(field.schema(), value, field.name());
            avroRecord.put(index, coerced);
        }

        boolean skip() {
            return skip;
        }
    }

    private static final class PkCollector {
        private final TablePlan plan;
        private final LinkedHashMap<String, Object> buffer;
        private GenericData.Record avroRecord;

        PkCollector(TablePlan plan) {
            this.plan = plan;
            int capacity = Math.max(4, plan.pkFieldIndices.length);
            this.buffer = new LinkedHashMap<>(capacity);
        }

        void bind(GenericData.Record avroRecord) {
            this.avroRecord = avroRecord;
            buffer.clear();
        }

        Map<String, Object> target() {
            return buffer;
        }

        void applyToRecord(TableName table) {
            if (avroRecord == null || buffer.isEmpty()) {
                return;
            }
            for (Map.Entry<String, Object> entry : buffer.entrySet()) {
                FieldPlan field = plan.field(entry.getKey());
                if (field == null) {
                    throw new IllegalStateException(
                            "PK '" + entry.getKey() + "' отсутствует в Avro схеме таблицы " +
                                    table.getNameWithNamespaceInclAsString());
                }
                Object value = entry.getValue();
                if (value == null) {
                    avroRecord.put(field.index, null);
                } else {
                    field.write(avroRecord, value);
                }
            }
            buffer.clear();
        }

        GenericData.Record currentRecord() {
            return avroRecord;
        }
    }

    /**
     * Минимальный потокобезопасный кэш qualifier → String.
     * synchronized только на короткой операции lookup, чтобы переиспользовать объект LookupKey
     * и не создавать мусор; вставки в карту выполняются без блокировки через ConcurrentHashMap.
     */
    private static final class QualifierCache {
        private final ConcurrentHashMap<AbstractKey, String> cache = new ConcurrentHashMap<>(64);
        private final LookupKey lookup = new LookupKey();

        synchronized String intern(byte[] array, int offset, int length) {
            if (length == 0) {
                return "";
            }
            LookupKey key = lookup;
            key.set(array, offset, length);
            try {
                String cached = cache.get(key);
                if (cached != null) {
                    return cached;
                }
                String created = new String(array, offset, length, StandardCharsets.UTF_8);
                OwnedKey stored = key.toOwned();
                String race = cache.putIfAbsent(stored, created);
                return race != null ? race : created;
            } finally {
                key.clear();
            }
        }

        private abstract static class AbstractKey {
            byte[] bytes;
            int offset;
            int length;
            int hash;

            final void reset(byte[] array, int off, int len) {
                this.bytes = array;
                this.offset = off;
                this.length = len;
                this.hash = Bytes.hashCode(array, off, len);
            }

            final void clear() {
                this.bytes = null;
                this.offset = 0;
                this.length = 0;
                this.hash = 0;
            }

            @Override
            public final int hashCode() {
                return hash;
            }

            @Override
            public final boolean equals(Object o) {
                if (this == o) return true;
                if (!(o instanceof AbstractKey)) return false;
                AbstractKey other = (AbstractKey) o;
                if (this.hash != other.hash || this.length != other.length) {
                    return false;
                }
                return Bytes.equals(this.bytes, this.offset, this.length,
                        other.bytes, other.offset, other.length);
            }
        }

        private static final class LookupKey extends AbstractKey {
            void set(byte[] array, int off, int len) {
                reset(array, off, len);
            }

            OwnedKey toOwned() {
                return new OwnedKey(bytes, offset, length);
            }
        }

        private static final class OwnedKey extends AbstractKey {
            OwnedKey(byte[] source, int off, int len) {
                byte[] copy = new byte[len];
                System.arraycopy(source, off, copy, 0, len);
                reset(copy, 0, len);
            }
        }
    }
}
