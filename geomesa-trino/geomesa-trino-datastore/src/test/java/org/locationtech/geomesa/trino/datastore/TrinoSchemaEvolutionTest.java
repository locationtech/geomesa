/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.Query;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.features.ScalaSimpleFeature;
import org.locationtech.geomesa.fs.data.FileSystemDataStore;
import org.locationtech.geomesa.fs.storage.core.StorageCatalog.CatalogUpdate;
import org.locationtech.geomesa.security.SecurityUtils;
import org.locationtech.geomesa.trino.datastore.testcontainers.GeoMesaTrinoContainer;
import org.locationtech.geomesa.trino.datastore.testcontainers.IcebergRestContainer;
import org.locationtech.geomesa.trino.datastore.testcontainers.SeaweedFsContainer;
import org.locationtech.geomesa.utils.geotools.FeatureUtils;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.locationtech.jts.geom.Point;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.MountableFile;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Schema evolution: a table is evolved through the file system data store, and then read back through trino.
 * Each test verifies that both the original data files (written before the change) and new data files (written
 * after the change) are readable with the updated schema.
 *
 * <p>Each change is verified against a new trino data store, and against an existing data store that cached
 * the schema before the change. The existing store only re-loads its schema when a query fails (i.e. a cached
 * column was renamed or removed) - for other changes it keeps using the cached schema, but must still return
 * all the data.
 */
public class TrinoSchemaEvolutionTest {

    public static final Network network = Network.newNetwork();

    @SuppressWarnings("resource")
    public static final TrinoContainer trino =
            new GeoMesaTrinoContainer()
                    .withGeoMesaPlugin()
                    .withNetwork(network)
                    .withNetworkAliases("trino")
                    .withFileSystemBind(MountableFile.forClasspathResource("docker/trino/spatial_iceberg.properties").getResolvedPath(),
                            "/etc/trino/catalog/spatial_iceberg.properties",
                            BindMode.READ_ONLY);

    public static final SeaweedFsContainer s3 =
            new SeaweedFsContainer("admin", "admin")
                    .withNetwork(network)
                    .withNetworkAliases("seaweed");

    public static final IcebergRestContainer iceberg =
            new IcebergRestContainer("admin", "admin")
                    .withNetwork(network)
                    .withNetworkAliases("rest-catalog");

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final AtomicInteger namespaces = new AtomicInteger();

    private static final String structuralType =
            "{\"type\":\"record\",\"name\":\"properties\",\"fields\":[{\"name\":\"count\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}";

    private static final SimpleFeatureType sft =
            SimpleFeatureTypes.createType("evolution-test",
                    "name:String,age:Int,json:String:json=true:json-schema='" + structuralType + "',*geom:Point:srid=4326,dtg:Date;" +
                            "geomesa.fs.scheme='z2:bits=8,day'");

    private static final String typeName = sft.getTypeName();

    private static final List<SimpleFeature> features = IntStream.range(0, 10).boxed().map(i -> {
        var sf = new ScalaSimpleFeature(sft, Integer.toString(i), null, null);
        sf.setAttribute(0, "name" + i);
        sf.setAttribute(1, i);
        sf.setAttribute(2, "{\"count\":" + i + ",\"name\":\"" + i + "name\"}");
        sf.setAttribute(3, "POINT(4" + i + " 5" + i + ")");
        sf.setAttribute(4, String.format("2014-01-%02dT00:00:01.000Z", i + 1));
        return (SimpleFeature) sf;
    }).toList();

    // filters to the indices of the features they match
    private static final Map<String, List<Integer>> queries = new LinkedHashMap<>();
    static {
        queries.put("INCLUDE", IntStream.range(0, 10).boxed().toList());
        queries.put("bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z",
                IntStream.range(0, 8).boxed().toList());
        queries.put("name = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z",
                List.of(5));
        queries.put("name = 'name5'", List.of(5));
        queries.put("age < 5", IntStream.range(0, 5).boxed().toList());
    }

    private static final List<String[]> transforms =
            Arrays.asList(Query.ALL_NAMES, new String[] { "name", "dtg", "geom" }, new String[] { "geom", "dtg" });

    @BeforeAll
    public static void beforeAll() {
        s3.start();
        iceberg.start();
        trino.start();
    }

    @AfterAll
    public static void afterAll() {
        trino.stop();
        iceberg.stop();
        s3.stop();
    }

    @Test
    public void testAddColumn() throws Exception {
        try (var table = new EvolvingTable()) {
            table.update().columns().add(SimpleFeatureTypes.createDescriptor("height:Float")).apply();
            var updated = table.fsds.getSchema(typeName);
            Assertions.assertEquals(sft.getAttributeCount() + 1, updated.getAttributeCount());
            var written = table.write(orig -> {
                var sf = ScalaSimpleFeature.retype(updated, orig);
                sf.setAttribute("height", orig.getAttribute("age") + "." + orig.getAttribute("age"));
                return sf;
            });
            // existing files return null for the new column
            var expected = table.expected(written, orig -> ScalaSimpleFeature.retype(updated, orig));
            table.verify(ds -> {
                var schema = ds.getSchema(typeName);
                Assertions.assertEquals("height", schema.getDescriptor(sft.getAttributeCount()).getLocalName());
                Assertions.assertEquals(Float.class, schema.getDescriptor("height").getType().getBinding());
                verifyQueries(ds, expected, UnaryOperator.identity());
                var withHeight = written.stream().filter(f -> (Float) f.getAttribute("height") > 5f).toList();
                Assertions.assertEquals(5, withHeight.size());
                verifyQuery(ds, "height > 5", Query.ALL_NAMES, withHeight);
                verifyQuery(ds, "height IS NULL", new String[] { "name", "height" }, expected.subList(0, features.size()));
            });
            table.verifyExistingReadable();
        }
    }

    @Test
    public void testRemoveColumn() throws Exception {
        try (var table = new EvolvingTable()) {
            table.update().columns().remove("age").apply();
            var updated = table.fsds.getSchema(typeName);
            Assertions.assertEquals(sft.getAttributeCount() - 1, updated.getAttributeCount());
            var written = table.write(orig -> ScalaSimpleFeature.retype(updated, orig));
            var expected = table.expected(written, orig -> ScalaSimpleFeature.retype(updated, orig));
            DataStoreCheck check = ds -> {
                Assertions.assertNull(ds.getSchema(typeName).getDescriptor("age"));
                verifyQueries(ds, expected, UnaryOperator.identity());
                Assertions.assertThrows(Exception.class, () -> readAll(ds, "age < 5", Query.ALL_NAMES));
                Assertions.assertThrows(Exception.class, () -> readAll(ds, "INCLUDE", new String[] { "age" }));
            };
            table.verify(check);
            table.verifyExistingRefreshes(check);
        }
    }

    @Test
    public void testRenameColumn() throws Exception {
        try (var table = new EvolvingTable()) {
            table.update().columns().rename("name", "firstName").apply();
            var updated = table.fsds.getSchema(typeName);
            // note: copy goes by attribute index instead of name
            var written = table.write(orig -> ScalaSimpleFeature.copy(updated, orig));
            var expected = table.expected(written, orig -> ScalaSimpleFeature.copy(updated, orig));
            DataStoreCheck check = ds -> {
                var schema = ds.getSchema(typeName);
                Assertions.assertNull(schema.getDescriptor("name"));
                Assertions.assertEquals(0, schema.indexOf("firstName"));
                Assertions.assertEquals(String.class, schema.getDescriptor("firstName").getType().getBinding());
                verifyQueries(ds, expected, rename("name", "firstName"));
                Assertions.assertThrows(Exception.class, () -> readAll(ds, "name = 'name5'", Query.ALL_NAMES));
            };
            table.verify(check);
            table.verifyExistingRefreshes(check);
        }
    }

    @Test
    public void testRenameGeometryColumn() throws Exception {
        try (var table = new EvolvingTable()) {
            table.update().columns().rename("geom", "location").apply();
            var updated = table.fsds.getSchema(typeName);
            var written = table.write(orig -> ScalaSimpleFeature.copy(updated, orig));
            var expected = table.expected(written, orig -> ScalaSimpleFeature.copy(updated, orig));
            DataStoreCheck check = ds -> {
                var schema = ds.getSchema(typeName);
                Assertions.assertNull(schema.getDescriptor("geom"));
                Assertions.assertEquals(3, schema.indexOf("location"));
                Assertions.assertEquals(Point.class, schema.getDescriptor("location").getType().getBinding());
                Assertions.assertEquals("location", schema.getGeometryDescriptor().getLocalName());
                verifyQueries(ds, expected, rename("geom", "location"));
                var bounds = ds.getFeatureSource(typeName).getBounds();
                Assertions.assertEquals(40d, bounds.getMinX(), 0.01);
                Assertions.assertEquals(49d, bounds.getMaxX(), 0.01);
                Assertions.assertEquals(50d, bounds.getMinY(), 0.01);
                Assertions.assertEquals(59d, bounds.getMaxY(), 0.01);
            };
            table.verify(check);
            table.verifyExistingRefreshes(check);
        }
    }

    @Test
    public void testRenameDefaultDateColumn() throws Exception {
        try (var table = new EvolvingTable()) {
            table.update().columns().rename("dtg", "ts").apply();
            var updated = table.fsds.getSchema(typeName);
            var written = table.write(orig -> ScalaSimpleFeature.copy(updated, orig));
            var expected = table.expected(written, orig -> ScalaSimpleFeature.copy(updated, orig));
            DataStoreCheck check = ds -> {
                var schema = ds.getSchema(typeName);
                Assertions.assertNull(schema.getDescriptor("dtg"));
                Assertions.assertEquals(4, schema.indexOf("ts"));
                Assertions.assertEquals(Date.class, schema.getDescriptor("ts").getType().getBinding());
                Assertions.assertEquals("ts", schema.getUserData().get(SimpleFeatureTypes.Configs$.MODULE$.DefaultDtgField()));
                verifyQueries(ds, expected, rename("dtg", "ts"));
            };
            table.verify(check);
            table.verifyExistingRefreshes(check);
        }
    }

    @Test
    public void testReorderColumn() throws Exception {
        try (var table = new EvolvingTable()) {
            table.update().columns().reorder("name", 1).apply();
            var updated = table.fsds.getSchema(typeName);
            var written = table.write(orig -> ScalaSimpleFeature.retype(updated, orig));
            var expected = table.expected(written, orig -> ScalaSimpleFeature.retype(updated, orig));
            table.verify(ds -> {
                Assertions.assertEquals(List.of("age", "name", "json", "geom", "dtg"), attributes(ds.getSchema(typeName)));
                verifyQueries(ds, expected, UnaryOperator.identity());
            });
            table.verifyExistingReadable();
        }
    }

    @Test
    public void testReorderColumnToLast() throws Exception {
        try (var table = new EvolvingTable()) {
            table.update().columns().reorder("name", sft.getAttributeCount() - 1).apply();
            var updated = table.fsds.getSchema(typeName);
            var written = table.write(orig -> ScalaSimpleFeature.retype(updated, orig));
            var expected = table.expected(written, orig -> ScalaSimpleFeature.retype(updated, orig));
            table.verify(ds -> {
                Assertions.assertEquals(List.of("age", "json", "geom", "dtg", "name"), attributes(ds.getSchema(typeName)));
                verifyQueries(ds, expected, UnaryOperator.identity());
            });
            table.verifyExistingReadable();
        }
    }

    @Test
    public void testWidenColumn() throws Exception {
        try (var table = new EvolvingTable()) {
            table.update().columns().update(SimpleFeatureTypes.createDescriptor("age:Long")).apply();
            var updated = table.fsds.getSchema(typeName);
            var written = table.write(orig -> ScalaSimpleFeature.retype(updated, orig));
            var expected = table.expected(written, orig -> ScalaSimpleFeature.retype(updated, orig));
            table.verify(ds -> {
                var schema = ds.getSchema(typeName);
                Assertions.assertEquals(sft.indexOf("age"), schema.indexOf("age"));
                Assertions.assertEquals(Long.class, schema.getDescriptor("age").getType().getBinding());
                verifyQueries(ds, expected, UnaryOperator.identity());
            });
            table.verifyExistingReadable();
        }
    }

    @Test
    public void testUpdateStructuralType() throws Exception {
        try (var table = new EvolvingTable()) {
            // drops 'count', keeps 'name', and adds 'nested'
            var updatedType =
                    "{\"name\":\"properties\",\"type\":\"record\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}," +
                            "{\"name\":\"nested\",\"type\":{\"name\":\"kv\",\"type\":\"record\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"}]}}]}";
            table.update().columns().update(
                    SimpleFeatureTypes.createDescriptor("json:String:json=true:json-schema='" + updatedType + "'")).apply();
            var updated = table.fsds.getSchema(typeName);
            var written = table.write(orig -> {
                var sf = ScalaSimpleFeature.retype(updated, orig);
                var i = orig.getAttribute("age");
                sf.setAttribute("json", "{\"name\":\"" + i + "name\",\"nested\":{\"key\":\"k" + i + "\"}}");
                return sf;
            });
            // existing files no longer return the dropped field, and return null for the added one
            var expected = table.expected(written, orig -> {
                var sf = ScalaSimpleFeature.retype(updated, orig);
                sf.setAttribute("json", ((String) orig.getAttribute("json")).replaceAll("\"count\":\\d+,", ""));
                return sf;
            });
            table.verify(ds -> {
                var schema = ds.getSchema(typeName);
                Assertions.assertEquals(sft.indexOf("json"), schema.indexOf("json"));
                Assertions.assertEquals(String.class, schema.getDescriptor("json").getType().getBinding());
                Assertions.assertEquals(updatedType, schema.getDescriptor("json").getUserData().get("json-schema"));
                for (var query : queries.entrySet()) {
                    for (var transform : Arrays.asList(Query.ALL_NAMES, new String[] { "geom", "dtg", "json" })) {
                        verifyQuery(ds, query.getKey(), transform, select(expected, query.getValue()));
                    }
                }
                // json-path filters are pushed down against the updated struct
                verifyQuery(ds, "\"$.json.name\" = '5name'", Query.ALL_NAMES, select(expected, List.of(5)));
                verifyQuery(ds, "\"$.json.nested.key\" = 'k5'", Query.ALL_NAMES, List.of(written.get(5)));
            });
            table.verifyExistingReadable();
        }
    }

    @Test
    public void testChangePartitions() throws Exception {
        try (var table = new EvolvingTable()) {
            table.update().table().setPartitions(JavaConverters.asScalaBuffer(List.of("z2:bits=4", "month")).toSeq()).apply();
            var written = table.write(ScalaSimpleFeature::copy);
            // existing files stay in the old partition spec, new files are written with the new one
            var expected = table.expected(written, Function.identity());
            table.verify(ds -> {
                Assertions.assertEquals(attributes(sft), attributes(ds.getSchema(typeName)));
                verifyQueries(ds, expected, UnaryOperator.identity());
            });
            table.verifyExistingReadable();
        }
    }

    @Test
    public void testRenameTable() throws Exception {
        try (var table = new EvolvingTable()) {
            var renamed = "renamed-test";
            table.update().table().rename(renamed).apply();
            // note: the existing store caches type names, so only check a new store
            var ds = table.connect();
            try {
                Assertions.assertEquals(List.of(renamed), Arrays.asList(ds.getTypeNames()));
                Assertions.assertEquals(attributes(sft), attributes(ds.getSchema(renamed)));
                Assertions.assertEquals(features.size(), ds.getFeatureSource(renamed).getCount(Query.ALL));
                for (var query : queries.entrySet()) {
                    for (var transform : transforms) {
                        var results = readAll(ds, renamed, query.getKey(), transform);
                        verifyFeatures(query.getKey(), transform, select(features, query.getValue()), results);
                    }
                }
            } finally {
                ds.dispose();
            }
        }
    }

    private static UnaryOperator<String> rename(String from, String to) {
        return s -> s.replaceAll("\\b" + from + "\\b", to);
    }

    private static List<String> attributes(SimpleFeatureType sft) {
        return sft.getAttributeDescriptors().stream().map(d -> d.getLocalName()).toList();
    }

    /**
     * Select the original features and their post-evolution copies, by original feature index. Expects the list
     * format returned by {@link EvolvingTable#expected(List, Function)}.
     */
    private static List<SimpleFeature> select(List<SimpleFeature> expected, List<Integer> indices) {
        var selected = new ArrayList<SimpleFeature>();
        for (var i : indices) {
            selected.add(expected.get(i));
            if (expected.size() > features.size()) {
                selected.add(expected.get(features.size() + i));
            }
        }
        return selected;
    }

    /**
     * Runs the standard filters and transforms
     *
     * @param ds trino data store
     * @param expected expected features, from {@link EvolvingTable#expected(List, Function)}
     * @param rename renames attributes in the filters and transforms, for when the schema has changed
     */
    private static void verifyQueries(DataStore ds, List<SimpleFeature> expected, UnaryOperator<String> rename) throws Exception {
        Assertions.assertEquals(expected.size(), ds.getFeatureSource(typeName).getCount(Query.ALL));
        for (var query : queries.entrySet()) {
            var filter = rename.apply(query.getKey());
            if (filter.contains("age") && expected.get(0).getFeatureType().getDescriptor("age") == null) {
                continue; // attribute was removed
            }
            for (var transform : transforms) {
                var names = transform == null ? null : Arrays.stream(transform).map(rename).toArray(String[]::new);
                verifyQuery(ds, filter, names, select(expected, query.getValue()));
            }
        }
    }

    private static void verifyQuery(DataStore ds, String filter, String[] transform, List<SimpleFeature> expected)
            throws Exception {
        verifyFeatures(filter, transform, expected, readAll(ds, filter, transform));
    }

    private static void verifyFeatures(String filter, String[] transform, List<SimpleFeature> expected, List<SimpleFeature> actual)
            throws Exception {
        var message = "filter: " + filter + ", transform: " + Arrays.toString(transform);
        var expectedIds = expected.stream().map(SimpleFeature::getID).sorted().toList();
        Assertions.assertEquals(expectedIds, actual.stream().map(SimpleFeature::getID).sorted().toList(), message);
        var byId = expected.stream().collect(Collectors.toMap(SimpleFeature::getID, f -> f));
        for (var feature : actual) {
            var expectedFeature = byId.get(feature.getID());
            var names = transform != null ? Arrays.asList(transform) : attributes(expectedFeature.getFeatureType());
            Assertions.assertEquals(names, attributes(feature.getFeatureType()), message);
            for (var name : names) {
                var expectedValue = expectedFeature.getAttribute(name);
                var actualValue = feature.getAttribute(name);
                if ("json".equals(name) && expectedValue != null && actualValue != null) {
                    // compare parsed trees so key ordering, whitespace and null fields don't matter
                    Assertions.assertEquals(parseJson((String) expectedValue), parseJson((String) actualValue),
                            message + ", id: " + feature.getID());
                } else {
                    Assertions.assertEquals(expectedValue, actualValue, message + ", id: " + feature.getID() + ", attribute: " + name);
                }
            }
        }
    }

    private static List<SimpleFeature> readAll(DataStore ds, String filter, String[] transform) throws Exception {
        return readAll(ds, typeName, filter, transform);
    }

    private static List<SimpleFeature> readAll(DataStore ds, String typeName, String filter, String[] transform) throws Exception {
        var results = new ArrayList<SimpleFeature>();
        var query = new Query(typeName, ECQL.toFilter(filter), transform);
        try (var reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
            while (reader.hasNext()) {
                results.add(reader.next());
            }
        }
        return results;
    }

    // parses json and removes any null-valued object keys, which are returned for fields missing from older files
    private static JsonNode parseJson(String json) throws Exception {
        return stripNulls(mapper.readTree(json));
    }

    private static JsonNode stripNulls(JsonNode node) {
        if (node instanceof ObjectNode object) {
            var names = new ArrayList<String>();
            object.fieldNames().forEachRemaining(names::add);
            for (var name : names) {
                if (object.get(name).isNull()) {
                    object.remove(name);
                } else {
                    stripNulls(object.get(name));
                }
            }
        }
        return node;
    }

    private interface DataStoreCheck {
        void check(DataStore ds) throws Exception;
    }

    /**
     * A table in its own namespace, populated with the test features, along with a trino data store that has
     * already loaded (and cached) the pre-evolution schema
     */
    private static class EvolvingTable implements AutoCloseable {

        private final String namespace = String.format("evolution%03d", namespaces.getAndIncrement());

        final FileSystemDataStore fsds;
        // a store that was opened (and has cached the schema) before the table was evolved
        final DataStore existing;

        EvolvingTable() throws Exception {
            var fsProps =
                    String.join("\n",
                            "type=rest",
                            "uri=http://" + iceberg.getHost() + ":" + iceberg.getFirstMappedPort() + "/",
                            "iceberg.namespace=" + namespace,
                            "fs.s3.region=us-east-1",
                            "fs.s3.endpoint=" + s3.getS3URL(),
                            "fs.s3.access-key-id=admin",
                            "fs.s3.secret-access-key=admin",
                            "fs.s3.force-path-style=true");
            fsds = (FileSystemDataStore) DataStoreFinder.getDataStore(Map.of("fs.config.properties", fsProps));
            Assertions.assertNotNull(fsds);
            DataStore existing = null;
            try {
                fsds.createSchema(sft);
                try (var writer = fsds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) {
                    for (var feature : features) {
                        FeatureUtils.write(writer, feature, true);
                    }
                    // validate that visibilities are still enforced after schema changes
                    var hidden = ScalaSimpleFeature.copy(features.get(0));
                    hidden.setId("-1");
                    SecurityUtils.setFeatureVisibility(hidden, "admin");
                    FeatureUtils.write(writer, hidden, true);
                }
                existing = connect();
                // prime the schema cache, and sanity check our baseline
                Assertions.assertEquals(attributes(sft), attributes(existing.getSchema(typeName)));
                verifyQueries(existing, features, UnaryOperator.identity());
            } catch (Throwable e) {
                if (existing != null) {
                    existing.dispose();
                }
                fsds.dispose();
                throw e;
            }
            this.existing = existing;
        }

        CatalogUpdate update() {
            return fsds.update(typeName);
        }

        DataStore connect() throws Exception {
            var ds = DataStoreFinder.getDataStore(Map.of(
                    TrinoDataStoreFactory.HOST.key, trino.getHost(),
                    TrinoDataStoreFactory.PORT.key, trino.getFirstMappedPort(),
                    TrinoDataStoreFactory.SCHEMA.key, namespace));
            Assertions.assertNotNull(ds);
            return ds;
        }

        /**
         * Writes a copy of each test feature using the current (evolved) schema
         *
         * @param evolve converts an original feature to the evolved schema
         * @return the features that were written
         */
        List<SimpleFeature> write(Function<SimpleFeature, SimpleFeature> evolve) throws Exception {
            var written = new ArrayList<SimpleFeature>();
            try (var writer = fsds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) {
                for (var feature : features) {
                    var evolved = ScalaSimpleFeature.copy(evolve.apply(feature));
                    evolved.setId("1_" + feature.getID());
                    FeatureUtils.write(writer, evolved, true);
                    written.add(evolved);
                }
            }
            return written;
        }

        /**
         * Gets the full set of expected features
         *
         * @param written features written after the schema change
         * @param evolve converts an original feature to how it should be returned under the evolved schema
         * @return the original features, followed by the written ones, each ordered by original feature index
         */
        List<SimpleFeature> expected(List<SimpleFeature> written, Function<SimpleFeature, SimpleFeature> evolve) {
            var expected = new ArrayList<SimpleFeature>();
            features.forEach(f -> expected.add(evolve.apply(f)));
            expected.addAll(written);
            return expected;
        }

        /**
         * Runs the check against a new data store, which loads the evolved schema
         */
        void verify(DataStoreCheck check) throws Exception {
            var ds = connect();
            try {
                check.check(ds);
            } finally {
                ds.dispose();
            }
        }

        /**
         * Runs the check against the existing data store, for changes that invalidate the cached schema (i.e.
         * a cached column no longer exists). The first read fails, which causes the store to re-load the schema
         * and retry - after that, it should behave the same as a new store.
         */
        void verifyExistingRefreshes(DataStoreCheck check) throws Exception {
            Assertions.assertEquals(attributes(sft), attributes(existing.getSchema(typeName)));
            Assertions.assertEquals(2 * features.size(), readAll(existing, "INCLUDE", Query.ALL_NAMES).size());
            check.check(existing);
        }

        /**
         * Verifies the existing data store, for changes that are still compatible with the cached schema. The
         * store doesn't detect the change, but should continue to return all the data using the cached schema.
         */
        void verifyExistingReadable() throws Exception {
            Assertions.assertEquals(attributes(sft), attributes(existing.getSchema(typeName)));
            Assertions.assertEquals(2 * features.size(), existing.getFeatureSource(typeName).getCount(Query.ALL));
            for (var query : queries.entrySet()) {
                for (var transform : transforms) {
                    var ids = readAll(existing, query.getKey(), transform).stream().map(SimpleFeature::getID).sorted().toList();
                    var expected = query.getValue().stream().flatMap(i -> Stream.of("" + i, "1_" + i)).sorted().toList();
                    Assertions.assertEquals(expected, ids, "filter: " + query.getKey() + ", transform: " + Arrays.toString(transform));
                }
            }
        }

        @Override
        public void close() {
            existing.dispose();
            fsds.dispose();
        }
    }
}
