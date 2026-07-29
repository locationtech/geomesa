/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.Query;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
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

    private static final SimpleFeatureType sft =
            SimpleFeatureTypes.createType("parquet", "name:String:fs.bounds=true,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.fs.scheme='daily,z2:bits=4'");

    private static final List<SimpleFeature> features = IntStream.range(0, 10).boxed().map(i -> {
        var sf = new ScalaSimpleFeature(sft, Integer.toString(i), null, null);
        sf.setAttribute(0, "test" + i);
        sf.setAttribute(1, 100 + i);
        sf.setAttribute(2, "2017-06-0" + (5 + (i % 3)) + "T04:03:02.0001Z");
        sf.setAttribute(3, "POINT(10 10." + i + ")");
        return (SimpleFeature) sf;
    }).toList();

    @BeforeClass
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

        var fsds = DataStoreFinder.getDataStore(Map.of("fs.path", "s3://geomesa/fs/iceberg/", "fs.config.properties", fsProps));
        try {
            fsds.createSchema(sft);
            try (var writer = fsds.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
                features.forEach(f -> FeatureUtils.write(writer, f, true));
            }
        } finally {
            fsds.dispose();
        }

        trino.start();
    }

    @AfterClass
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
            Assertions.assertArrayEquals(new String[]{ sft.getTypeName() }, ds.getTypeNames());
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

}
