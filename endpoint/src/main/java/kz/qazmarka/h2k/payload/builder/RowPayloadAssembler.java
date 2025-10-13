package kz.qazmarka.h2k.payload.builder;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.H2kConfig.TableOptionsSnapshot;
import kz.qazmarka.h2k.config.H2kConfig.ValueSource;
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
    private static final String FIELD_EVENT_TS = "_event_ts";
    private static final String FIELD_DELETE = "_delete";

    private final Decoder decoder;
    private final H2kConfig cfg;
    private final QualifierCache qualifierCache = new QualifierCache();
    private final Set<String> tableOptionsLogged = ConcurrentHashMap.newKeySet();
    private final TablePlanCache tablePlans;

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

        PkCollector pkCollector = plan.borrowCollector();
        try {
            pkCollector.bind(avroRecord);
            decodePrimaryKey(table, rowKey, plan, pkCollector);
        } finally {
            plan.releaseCollector(pkCollector);
        }

        long maxTimestamp = Long.MIN_VALUE;
        boolean hasDelete = false;

        if (cells != null) {
            for (Cell cell : cells) {
                maxTimestamp = Math.max(maxTimestamp, cell.getTimestamp());
                if (CellUtil.isDelete(cell)) {
                    hasDelete = true;
                } else {
                    writeCellValue(table, plan, avroRecord, cell);
                }
            }
        }

        if (plan.eventTsIndex >= 0 && maxTimestamp != Long.MIN_VALUE) {
            avroRecord.put(plan.eventTsIndex, maxTimestamp);
        }
        if (plan.deleteIndex >= 0 && hasDelete) {
            avroRecord.put(plan.deleteIndex, Boolean.TRUE);
        }

        plan.applyWalMetadata(avroRecord, walSeq, walWriteTime);

        return avroRecord;
    }

    private void writeCellValue(TableName table,
                                TablePlan plan,
                                GenericData.Record avroRecord,
                                Cell cell) {
        FieldPlan field = resolveField(plan, cell);
        if (field == null) {
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

    private FieldPlan resolveField(TablePlan plan, Cell cell) {
        String column = qualifierCache.intern(
                cell.getQualifierArray(),
                cell.getQualifierOffset(),
                cell.getQualifierLength());
        return plan.field(column);
    }

    private void decodePrimaryKey(TableName table,
                                  RowKeySlice rowKey,
                                  TablePlan plan,
                                  PkCollector collector) {
        if (rowKey == null) {
            throw new IllegalStateException(
                    "Отсутствует rowkey для таблицы " + table.getNameWithNamespaceInclAsString());
        }
        decoder.decodeRowKey(table, rowKey, plan.saltBytes, collector);
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
        H2kConfig.CfFilterSnapshot cfSnapshot = snapshot.cfFilter();
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

    private static String label(ValueSource source) {
        return source == null ? "" : source.label();
    }

    /**
     * Кеш табличных планов (Avro-схема, индексы полей, PK, служебные поля).
     */
    private static final class TablePlanCache {
        private final H2kConfig cfg;
        private final AvroSchemaRegistry schemaRegistry;
        private final ConcurrentHashMap<String, TablePlan> cache = new ConcurrentHashMap<>();

        TablePlanCache(H2kConfig cfg, AvroSchemaRegistry schemaRegistry) {
            this.cfg = cfg;
            this.schemaRegistry = schemaRegistry;
        }

        TablePlan planFor(TableName table) {
            String key = table.getNameWithNamespaceInclAsString();
            return cache.computeIfAbsent(key, k -> buildPlan(table));
        }

        private TablePlan buildPlan(TableName table) {
            Schema schema = loadSchema(table);
            Map<String, FieldPlan> fields = buildFieldPlans(schema);

            String[] pkColumns = cfg.primaryKeyColumns(table);
            int[] pkIndices = new int[pkColumns.length];
            for (int i = 0; i < pkColumns.length; i++) {
                String col = pkColumns[i];
                FieldPlan plan = lookupField(fields, col);
                if (plan == null) {
                    throw new IllegalStateException(
                            "Avro: PK колонка '" + col + "' отсутствует в схеме " + table.getNameAsString());
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

        private Map<String, FieldPlan> buildFieldPlans(Schema schema) {
            Map<String, FieldPlan> map = new HashMap<>(schema.getFields().size() * 2);
            for (Schema.Field field : schema.getFields()) {
                FieldPlan plan = new FieldPlan(field);
                map.put(field.name(), plan);
                map.put(field.name().toUpperCase(Locale.ROOT), plan);
                map.put(field.name().toLowerCase(Locale.ROOT), plan);
            }
            return map;
        }

        private FieldPlan lookupField(Map<String, FieldPlan> fields, String name) {
            FieldPlan plan = fields.get(name);
            if (plan != null) return plan;
            if (name == null) return null;
            FieldPlan upper = fields.get(name.toUpperCase(Locale.ROOT));
            if (upper != null) return upper;
            return fields.get(name.toLowerCase(Locale.ROOT));
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
        private final PkCollector collector;

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
            this.collector = new PkCollector(this);
        }

        GenericData.Record newRecord() {
            GenericData.Record avroRecord = new GenericData.Record(schema);
            if (deleteIndex >= 0) {
                avroRecord.put(deleteIndex, Boolean.FALSE);
            }
            return avroRecord;
        }

        FieldPlan field(String name) {
            FieldPlan plan = fields.get(name);
            if (plan != null) {
                return plan;
            }
            if (name == null) {
                return null;
            }
            return fields.get(name.toUpperCase(Locale.ROOT));
        }

        PkCollector borrowCollector() {
            return collector;
        }

        void releaseCollector(PkCollector collector) {
            if (collector != null) {
                collector.unbind();
            }
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

        FieldPlan(Schema.Field field) {
            this.field = field;
            this.index = field.pos();
        }

        void write(GenericData.Record avroRecord, Object value) {
            Object coerced = AvroValueCoercer.coerceValue(field.schema(), value, field.name());
            avroRecord.put(index, coerced);
        }
    }

    private static final class PkCollector implements Map<String, Object> {
        private final TablePlan plan;
        private GenericData.Record avroRecord;
        private int size;

        PkCollector(TablePlan plan) {
            this.plan = plan;
        }

        void bind(GenericData.Record avroRecord) {
            this.avroRecord = avroRecord;
            this.size = 0;
        }

        void unbind() {
            this.avroRecord = null;
            this.size = 0;
        }

        GenericData.Record currentRecord() {
            return avroRecord;
        }

        @Override
        public Object put(String key, Object value) {
            FieldPlan field = plan.field(key);
            if (field == null || avroRecord == null) {
                return null;
            }
            Object previous = avroRecord.get(field.index);
            field.write(avroRecord, value);
            if (previous == null && avroRecord.get(field.index) != null) {
                size++;
            }
            return previous;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean isEmpty() {
            return size == 0;
        }

        @Override
        public boolean containsKey(Object key) {
            if (!(key instanceof String) || avroRecord == null) {
                return false;
            }
            FieldPlan field = plan.field((String) key);
            return field != null && avroRecord.get(field.index) != null;
        }

        @Override
        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get(Object key) {
            if (!(key instanceof String) || avroRecord == null) {
                return null;
            }
            FieldPlan field = plan.field((String) key);
            if (field == null) {
                return null;
            }
            return avroRecord.get(field.index);
        }

        @Override
        public Object remove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putAll(Map<? extends String, ?> m) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> keySet() {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.Collection<Object> values() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return Collections.emptySet();
        }
    }

    /** Минимальный потокобезопасный кэш qualifier → String. */
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
