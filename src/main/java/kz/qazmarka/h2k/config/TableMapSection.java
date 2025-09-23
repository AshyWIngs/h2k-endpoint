package kz.qazmarka.h2k.config;

import java.util.Map;

import kz.qazmarka.h2k.util.Parsers;

import org.apache.hadoop.conf.Configuration;

public final class TableMapSection {
    final Map<String, Integer> saltMap;
    final Map<String, Integer> capacityHints;

    private TableMapSection(Map<String, Integer> saltMap,
                            Map<String, Integer> capacityHints) {
        this.saltMap = saltMap;
        this.capacityHints = capacityHints;
    }

    static TableMapSection from(Configuration cfg) {
        Map<String, Integer> saltMap = Parsers.readSaltMap(cfg, H2kConfig.K_SALT_MAP);
        Map<String, Integer> capacityHints = Parsers.readCapacityHints(cfg, H2kConfig.Keys.CAPACITY_HINTS, H2kConfig.Keys.CAPACITY_HINT_PREFIX);
        return new TableMapSection(saltMap, capacityHints);
    }
}
