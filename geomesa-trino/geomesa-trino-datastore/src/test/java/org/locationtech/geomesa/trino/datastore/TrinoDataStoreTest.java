/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.Query;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.features.ScalaSimpleFeature;
import org.locationtech.geomesa.trino.datastore.testcontainers.GeoMesaTrinoContainer;
import org.locationtech.geomesa.trino.datastore.testcontainers.IcebergRestContainer;
import org.locationtech.geomesa.utils.geotools.FeatureUtils;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TrinoDataStoreTest {

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

    public static final MinIOContainer minio =
            new MinIOContainer(DockerImageName.parse("minio/minio").withTag(System.getProperty("minio.docker.tag")))
                    .withUserName("minioadmin")
                    .withPassword("minioadmin")
                    .withNetwork(network)
                    .withNetworkAliases("minio");

    public static final IcebergRestContainer iceberg =
            new IcebergRestContainer(minio.getUserName(), minio.getPassword())
                    .withNetwork(network)
                    .withNetworkAliases("rest-catalog");

    // note: the dash in the name tests feature type names that don't exactly map to table names
    private static final SimpleFeatureType sft =
            SimpleFeatureTypes.createType("parquet-test", "name:String,age:Int,props:String:json=true,dtg:Date,*geom:Point:srid=4326;geomesa.fs.scheme='daily,z2:bits=4'");

    private static final List<SimpleFeature> features = IntStream.range(0, 10).boxed().map(i -> {
        var sf = new ScalaSimpleFeature(sft, Integer.toString(i), null, null);
        sf.setAttribute(0, "test" + i);
        sf.setAttribute(1, 100 + i);
        sf.setAttribute(2, "{\"weight\":" + i + "}");
        sf.setAttribute(3, "2017-06-0" + (5 + (i % 3)) + "T04:03:02.0001Z");
        sf.setAttribute(4, "POINT(10 10." + i + ")");
        return (SimpleFeature) sf;
    }).toList();

    // avro schema describing the structural shape of the json=true 'props' attribute - stored as an
    // iceberg struct, which trino reads back as a row and renders as a json document
    private static final String jsonAvro =
            "{" +
            "  \"type\": \"record\"," +
            "  \"name\": \"props\"," +
            "  \"fields\": [" +
            "    { \"name\": \"name\", \"type\": [\"null\", \"string\"], \"default\": null }," +
            "    { \"name\": \"age\", \"type\": \"int\" }," +
            "    { \"name\": \"tags\", \"type\": { \"type\": \"array\", \"items\": \"string\" } }," +
            "    { \"name\": \"scores\", \"type\": { \"type\": \"map\", \"values\": \"long\" } }," +
            "    { \"name\": \"nested\", \"type\": [\"null\", {" +
            "        \"type\": \"record\"," +
            "        \"name\": \"nested\"," +
            "        \"fields\": [ { \"name\": \"flag\", \"type\": \"boolean\" } ]" +
            "    }], \"default\": null }" +
            "  ]" +
            "}";

    private static final SimpleFeatureType jsonSft =
            SimpleFeatureTypes.createType("json-test", "props:String:json=true,dtg:Date,*geom:Point:srid=4326;geomesa.fs.scheme='daily,z2:bits=4'");
    static {
        // "json-schema" mirrors SimpleFeatureTypes.AttributeOptions.OptJsonSchema
        jsonSft.getDescriptor("props").getUserData().put("json-schema", jsonAvro);
    }

    private static final List<String> jsonValues = Arrays.asList(
            "{\"name\":\"alice\",\"age\":30,\"tags\":[\"a\",\"b\"],\"scores\":{\"x\":1,\"y\":2},\"nested\":{\"flag\":true}}",
            // omitted optional fields (name, nested), empty array and map
            "{\"age\":7,\"tags\":[],\"scores\":{}}",
            // explicit nulls for optional fields
            "{\"name\":null,\"age\":99,\"tags\":[\"z\"],\"scores\":{\"k\":42},\"nested\":null}",
            "{\"name\":\"dave\",\"age\":11,\"tags\":[\"p\",\"q\",\"r\"],\"scores\":{\"a\":10},\"nested\":{\"flag\":false}}",
            null // null json value -> null attribute
    );

    private static final List<SimpleFeature> jsonFeatures = IntStream.range(0, jsonValues.size()).boxed().map(i -> {
        var sf = new ScalaSimpleFeature(jsonSft, Integer.toString(i), null, null);
        sf.setAttribute(0, jsonValues.get(i));
        sf.setAttribute(1, "2014-01-0" + (i + 1) + "T00:00:01.000Z");
        sf.setAttribute(2, "POINT(4" + i + " 5" + i + ")");
        return (SimpleFeature) sf;
    }).toList();

    // avro schema for a json=true attribute whose top-level value is an array of records - stored as an
    // iceberg list of structs, which trino reads back and renders as a json array
    private static final String jsonArrayAvro =
            "{" +
            "  \"type\": \"array\"," +
            "  \"items\": {" +
            "    \"type\": \"record\"," +
            "    \"name\": \"item\"," +
            "    \"fields\": [" +
            "      { \"name\": \"id\", \"type\": \"int\" }," +
            "      { \"name\": \"label\", \"type\": [\"null\", \"string\"], \"default\": null }" +
            "    ]" +
            "  }" +
            "}";

    private static final SimpleFeatureType jsonArraySft =
            SimpleFeatureTypes.createType("json-array-test", "props:String:json=true,dtg:Date,*geom:Point:srid=4326;geomesa.fs.scheme='daily,z2:bits=4'");
    static {
        jsonArraySft.getDescriptor("props").getUserData().put("json-schema", jsonArrayAvro);
    }

    private static final List<String> jsonArrayValues = Arrays.asList(
            "[{\"id\":1,\"label\":\"a\"},{\"id\":2,\"label\":\"b\"}]",
            "[]", // empty array
            // omitted optional field on the record
            "[{\"id\":42}]",
            null // null json value -> null attribute
    );

    private static final List<SimpleFeature> jsonArrayFeatures = IntStream.range(0, jsonArrayValues.size()).boxed().map(i -> {
        var sf = new ScalaSimpleFeature(jsonArraySft, Integer.toString(i), null, null);
        sf.setAttribute(0, jsonArrayValues.get(i));
        sf.setAttribute(1, "2014-01-0" + (i + 1) + "T00:00:01.000Z");
        sf.setAttribute(2, "POINT(4" + i + " 5" + i + ")");
        return (SimpleFeature) sf;
    }).toList();

    @BeforeAll
    public static void beforeAll() throws Exception {
        minio.start();
        minio.execInContainer("mc", "alias", "set", "localhost", "http://localhost:9000", minio.getUserName(), minio.getPassword());
        minio.execInContainer("mc", "mb", "localhost/geomesa");
        iceberg.start();

        var fsProps =
                String.join("\n",
                        "type=rest",
                        "uri=http://" + iceberg.getHost() + ":" + iceberg.getFirstMappedPort() + "/",
                        "iceberg.namespace=geomesa",
                        "fs.s3.region=us-east-1",
                        "fs.s3.endpoint=" + minio.getS3URL(),
                        "fs.s3.access-key-id=" + minio.getUserName(),
                        "fs.s3.secret-access-key=" + minio.getPassword(),
                        "fs.s3.force-path-style=true");

        var fsds = DataStoreFinder.getDataStore(Map.of("fs.config.properties", fsProps));
        try {
            fsds.createSchema(sft);
            try (var writer = fsds.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
                features.forEach(f -> FeatureUtils.write(writer, f, true));
            }
            fsds.createSchema(jsonSft);
            try (var writer = fsds.getFeatureWriterAppend(jsonSft.getTypeName(), Transaction.AUTO_COMMIT)) {
                jsonFeatures.forEach(f -> FeatureUtils.write(writer, f, true));
            }
            fsds.createSchema(jsonArraySft);
            try (var writer = fsds.getFeatureWriterAppend(jsonArraySft.getTypeName(), Transaction.AUTO_COMMIT)) {
                jsonArrayFeatures.forEach(f -> FeatureUtils.write(writer, f, true));
            }
        } finally {
            fsds.dispose();
        }

        trino.start();
    }

    @AfterAll
    public static void afterAll() {
        trino.stop();
        iceberg.stop();
        minio.stop();
    }

    @Test
    public void test() throws IOException, CQLException {
        var params = Map.of(
                TrinoDataStoreFactory.HOST.key, trino.getHost(),
                TrinoDataStoreFactory.PORT.key, trino.getFirstMappedPort(),
                TrinoDataStoreFactory.SCHEMA.key, "geomesa"
        );
        var ds = DataStoreFinder.getDataStore(params);
        Assertions.assertNotNull(ds);
        try {
            Assertions.assertTrue(Arrays.asList(ds.getTypeNames()).contains(sft.getTypeName()));
            var fs = ds.getFeatureSource(sft.getTypeName());
            Assertions.assertNotNull(fs);
            Assertions.assertEquals(features.size(), fs.getCount(Query.ALL));
            var bounds = fs.getBounds();
            Assertions.assertTrue(Math.abs(bounds.getMinX() - 10) < 0.01);
            Assertions.assertTrue(Math.abs(bounds.getMaxX() - 10) < 0.01);
            Assertions.assertTrue(Math.abs(bounds.getMinY() - 10) < 0.01);
            Assertions.assertTrue(Math.abs(bounds.getMaxY() - 10.9) < 0.01);

            var results = new ArrayList<SimpleFeature>(10);
            try (var query = fs.getFeatures(new Query(sft.getTypeName())).features()) {
                while (query.hasNext()) {
                    results.add(query.next());
                }
                results.sort(Comparator.comparing(SimpleFeature::getID));
            }
            Assertions.assertEquals(features.size(), results.size());
            for (var i = 0; i < features.size(); i++) {
                Assertions.assertTrue(ScalaSimpleFeature.equalIdAndAttributes(features.get(i), results.get(i)));
            }

            var filters = List.of(
                "INCLUDE",
                "name IN (" + features.stream().map(f-> "'" + f.getAttribute("name") + "'").collect(Collectors.joining(", ")) + ")",
                "bbox(geom, 5, 5, 15, 15)",
                "dtg DURING 2017-06-05T04:03:00.0000Z/2017-06-07T04:04:00.0000Z",
                "dtg > '2017-06-05T04:03:00.0000Z' AND dtg < '2017-06-07T04:04:00.0000Z'",
                "dtg DURING 2017-06-05T04:03:00.0000Z/2017-06-07T04:04:00.0000Z and bbox(geom, 5, 5, 15, 15)");
            var transforms = Arrays.asList(Query.ALL_NAMES, new String[] { "name" }, new String[] { "dtg", "geom" });

            for (String filter : filters) {
                for (String[] transform : transforms) {
                    results.clear();
                    var query = new Query(sft.getTypeName(), ECQL.toFilter(filter), transform);
                    try (var reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                        while (reader.hasNext()) {
                            results.add(reader.next());
                        }
                        results.sort(Comparator.comparing(SimpleFeature::getID));
                    }
                    Assertions.assertEquals(features.size(), results.size());
                    if (transform == Query.ALL_NAMES) {
                        for (var i = 0; i < features.size(); i++) {
                            Assertions.assertTrue(ScalaSimpleFeature.equalIdAndAttributes(features.get(i), results.get(i)));
                        }
                    } else {
                        for (var i = 0; i < results.size(); i++) {
                            Assertions.assertEquals(features.get(i).getID(), results.get(i).getID());
                            Assertions.assertEquals(transform.length, results.get(i).getAttributeCount());
                            for (var p : transform) {
                                Assertions.assertEquals(features.get(i).getAttribute(p), results.get(i).getAttribute(p));
                            }
                        }
                    }
                }
            }
        } finally {
            ds.dispose();
        }
    }

    @Test
    public void testStructuralJson() throws IOException, CQLException {
        var params = Map.of(
                TrinoDataStoreFactory.HOST.key, trino.getHost(),
                TrinoDataStoreFactory.PORT.key, trino.getFirstMappedPort(),
                TrinoDataStoreFactory.SCHEMA.key, "geomesa"
        );
        var ds = DataStoreFinder.getDataStore(params);
        Assertions.assertNotNull(ds);
        try {
            Assertions.assertTrue(Arrays.asList(ds.getTypeNames()).contains(jsonSft.getTypeName()));
            var fs = ds.getFeatureSource(jsonSft.getTypeName());
            Assertions.assertNotNull(fs);
            Assertions.assertEquals(jsonFeatures.size(), fs.getCount(Query.ALL));

            var results = new ArrayList<SimpleFeature>(jsonFeatures.size());
            try (var query = fs.getFeatures(new Query(jsonSft.getTypeName())).features()) {
                while (query.hasNext()) {
                    results.add(query.next());
                }
                results.sort(Comparator.comparing(SimpleFeature::getID));
            }
            Assertions.assertEquals(jsonFeatures.size(), results.size());

            var mapper = new ObjectMapper();
            for (var i = 0; i < jsonFeatures.size(); i++) {
                var expected = jsonFeatures.get(i);
                var actual = results.get(i);
                Assertions.assertEquals(expected.getID(), actual.getID());
                var expectedJson = (String) expected.getAttribute("props");
                var actualJson = (String) actual.getAttribute("props");
                if (expectedJson == null) {
                    Assertions.assertNull(actualJson);
                } else {
                    // compare parsed trees so key ordering / whitespace don't matter, normalizing away
                    // explicit nulls for optional fields, which the structural round-trip drops
                    Assertions.assertEquals(normalize(mapper, expectedJson), normalize(mapper, actualJson));
                }
            }
        } finally {
            ds.dispose();
        }
    }

    @Test
    public void testStructuralJsonArray() throws IOException, CQLException {
        var params = Map.of(
                TrinoDataStoreFactory.HOST.key, trino.getHost(),
                TrinoDataStoreFactory.PORT.key, trino.getFirstMappedPort(),
                TrinoDataStoreFactory.SCHEMA.key, "geomesa"
        );
        var ds = DataStoreFinder.getDataStore(params);
        Assertions.assertNotNull(ds);
        try {
            Assertions.assertTrue(Arrays.asList(ds.getTypeNames()).contains(jsonArraySft.getTypeName()));
            var fs = ds.getFeatureSource(jsonArraySft.getTypeName());
            Assertions.assertNotNull(fs);
            Assertions.assertEquals(jsonArrayFeatures.size(), fs.getCount(Query.ALL));

            var results = new ArrayList<SimpleFeature>(jsonArrayFeatures.size());
            try (var query = fs.getFeatures(new Query(jsonArraySft.getTypeName())).features()) {
                while (query.hasNext()) {
                    results.add(query.next());
                }
                results.sort(Comparator.comparing(SimpleFeature::getID));
            }
            Assertions.assertEquals(jsonArrayFeatures.size(), results.size());

            var mapper = new ObjectMapper();
            for (var i = 0; i < jsonArrayFeatures.size(); i++) {
                var expected = jsonArrayFeatures.get(i);
                var actual = results.get(i);
                Assertions.assertEquals(expected.getID(), actual.getID());
                var expectedJson = (String) expected.getAttribute("props");
                var actualJson = (String) actual.getAttribute("props");
                if (expectedJson == null) {
                    Assertions.assertNull(actualJson);
                } else {
                    // compare parsed trees so key ordering / whitespace don't matter, normalizing away
                    // explicit nulls for optional fields, which the structural round-trip drops
                    Assertions.assertEquals(normalize(mapper, expectedJson), normalize(mapper, actualJson));
                }
            }
        } finally {
            ds.dispose();
        }
    }

    // parses json and recursively removes any object keys whose value is null, so features that omit an
    // optional field and features that set it explicitly null compare equal. trino renders a struct with
    // all of its fields, including omitted-optional fields as explicit nulls, at any nesting depth.
    private static JsonNode normalize(ObjectMapper mapper, String json) throws IOException {
        return normalize(mapper.readTree(json));
    }

    private static JsonNode normalize(JsonNode node) {
        if (node instanceof ObjectNode object) {
            var nullFields = new ArrayList<String>();
            object.fieldNames().forEachRemaining(name -> {
                if (object.get(name).isNull()) {
                    nullFields.add(name);
                }
            });
            nullFields.forEach(object::remove);
            object.fields().forEachRemaining(e -> normalize(e.getValue()));
        } else if (node instanceof ArrayNode array) {
            array.forEach(TrinoDataStoreTest::normalize);
        }
        return node;
    }

}
