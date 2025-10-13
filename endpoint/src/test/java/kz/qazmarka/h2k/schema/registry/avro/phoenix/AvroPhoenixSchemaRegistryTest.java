package kz.qazmarka.h2k.schema.registry.avro.phoenix;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Paths;

import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

class AvroPhoenixSchemaRegistryTest {

    private static final TableName TABLE_AVRO = TableName.valueOf("T_AVRO");

    @Test
    void shouldReadTypesFromAvroSchema() {
        AvroSchemaRegistry avro = new AvroSchemaRegistry(Paths.get("src", "test", "resources", "avro"));
        AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(avro);

        assertEquals("VARCHAR", registry.columnType(TABLE_AVRO, "value"));
        assertNull(registry.columnType(TABLE_AVRO, "missing"));
        assertArrayEquals(new String[]{"id"}, registry.primaryKeyColumns(TABLE_AVRO));
        PhoenixTableMetadataProvider provider = registry;
        assertEquals(Integer.valueOf(0), provider.saltBytes(TABLE_AVRO));
        assertEquals(Integer.valueOf(4), provider.capacityHint(TABLE_AVRO));
    }
}
