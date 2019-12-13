/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.nativeapi;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.geomesa.api.DefaultSimpleFeatureView;
import org.locationtech.geomesa.api.GeoMesaIndex;
import org.locationtech.geomesa.api.GeoMesaQuery;
import org.locationtech.geomesa.api.SimpleFeatureView;
import org.locationtech.geomesa.api.ValueSerializer;
import org.locationtech.geomesa.hbase.data.HBaseDataStore;
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex;
import org.locationtech.geomesa.utils.index.IndexMode$;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option$;

import javax.annotation.Nullable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class GeoMesaIndexTest {

    private static final Logger logger = LoggerFactory.getLogger(GeoMesaIndex.class);

    public static class DomainObject {
        public final String id;
        public final int intValue;
        public final double doubleValue;

        public DomainObject(String id, int intValue, double doubleValue) {
            this.id = id;
            this.intValue = intValue;
            this.doubleValue = doubleValue;
        }
    }

    public static class DomainObjectValueSerializer implements ValueSerializer<DomainObject> {
        public static final Gson gson = new Gson();
        @Override
        public byte[] toBytes(DomainObject o) {
            return gson.toJson(o).getBytes();
        }

        @Override
        public DomainObject fromBytes(byte[] bytes) {
            return gson.fromJson(new String(bytes), DomainObject.class);
        }
    }

    final DomainObject one = new DomainObject("1", 1, 1.0);
    final DomainObject two = new DomainObject("2", 2, 2.0);
    final GeometryFactory gf = JTSFactoryFinder.getGeometryFactory();

    private static final HBaseTestingUtility testingUtil = new HBaseTestingUtility();
    static Connection userConn = null;
    static Connection adminConn = null;
    static User adminUser = null;
    static User testUser = null;

    @BeforeClass
    public static void setup() throws Exception {
        testingUtil.getConfiguration().set("hbase.superuser", "admin");
        testingUtil.getConfiguration().set("hfile.format.version", "3");
        testingUtil.startMiniCluster(1);

        adminUser = User.createUserForTesting(testingUtil.getConfiguration(), "admin", new String[] {"supergroup"});
        testUser = User.createUserForTesting(testingUtil.getConfiguration(), "testUser", new String[] {});

        adminUser.runAs(new PrivilegedExceptionAction<Object>(){
            @Override
            public Object run() throws Exception {
                testingUtil.waitTableAvailable(TableName.valueOf("hbase:labels"), 50000);
                adminConn = ConnectionFactory.createConnection(testingUtil.getConfiguration());
                try {
                    VisibilityClient.addLabels(adminConn, new String[]{ "user", "admin" });
                } catch (Throwable throwable) {
                    throw new Exception(throwable);
                }
                testingUtil.waitLabelAvailable(10000, "user", "admin");

                try {
                    VisibilityClient.setAuths(adminConn, new String[]{ "user" }, "testUser");
                } catch (Throwable throwable) {
                    throw new Exception(throwable);
                }
                return null;
            }
        });

        testUser.runAs(new PrivilegedExceptionAction<Object>(){
            @Override
            public Object run() throws Exception {
                userConn = ConnectionFactory.createConnection(testingUtil.getConfiguration());
                return null;
            }
        });

        logger.info("HBase Testing Cluster Started");
    }

    @AfterClass
    public static void teardown() throws Exception {
        logger.info("try shutdown mini cluster");
        testingUtil.shutdownMiniCluster();
        logger.info("Shutdown mini cluster");
    }

    @Test
    public void testNativeAPI() {

        final GeoMesaIndex<DomainObject> index =
                HBaseGeoMesaIndex.build(
                        "hello",
                        false,
                        userConn,
                        new DomainObjectValueSerializer(),
                        new DefaultSimpleFeatureView<DomainObject>());

        index.insert(
                one.id,
                one,
                gf.createPoint(new Coordinate(-78.0, 38.0)),
                date("2016-01-01T12:15:00.000Z"));
        index.insert(
                two.id,
                two,
                gf.createPoint(new Coordinate(-78.0, 40.0)),
                date("2016-02-01T12:15:00.000Z"));
        index.flush();
        final GeoMesaQuery q =
                GeoMesaQuery.GeoMesaQueryBuilder.builder()
                        .within(-79.0, 37.0, -77.0, 39.0)
                        .during(date("2016-01-01T00:00:00.000Z"), date("2016-03-01T00:00:00.000Z"))
                        .build();
        final Iterable<DomainObject> results = index.query(q);

        final Iterable<String> ids = Iterables.transform(results, new Function<DomainObject, String>() {
            @Nullable
            @Override
            public String apply(@Nullable DomainObject domainObject) {
                return domainObject.id;
            }
        });
        Assert.assertArrayEquals("Invalid results", new String[] { "1" }, Iterables.toArray(ids, String.class));

        index.delete("1");

        final Iterable<DomainObject> resultsAfterDelete = index.query(q);
        final Iterable<String> idsPostDelete = Iterables.transform(resultsAfterDelete, new Function<DomainObject, String>() {
            @Nullable
            @Override
            public String apply(@Nullable DomainObject domainObject) {
                System.out.println("Got id " + domainObject.id);
                return domainObject.id;
            }
        });
        Assert.assertArrayEquals("Invalid results", new String[] { }, Iterables.toArray(idsPostDelete, String.class));
    }

    @Test
    public void testVisibilityNativeAPI() throws Throwable {
        // Note we are inserting here with adminConn bc it has both visibilities!
        final GeoMesaIndex<DomainObject> index =
                HBaseGeoMesaIndex.buildDefaultView("securityTest", false, adminConn, new DomainObjectValueSerializer());

        Map<String, Object> visibility = ImmutableMap.<String,Object>of(GeoMesaIndex.VISIBILITY, "user");
        index.insert(
                one.id,
                one,
                gf.createPoint(new Coordinate(-78.0, 38.0)),
                date("2016-01-01T12:15:00.000Z"),
                visibility);

        Map<String, Object> visibility2 = ImmutableMap.<String,Object>of(GeoMesaIndex.VISIBILITY, "admin");
        index.insert(
                two.id,
                two,
                gf.createPoint(new Coordinate(-78.0, 40.0)),
                date("2016-02-01T12:15:00.000Z"),
                visibility2);
        index.flush();

        final Iterable<DomainObject> results = index.query(GeoMesaQuery.include());
        Assert.assertEquals(2, Iterables.size(results));

        // Query again at the lower level.
        final GeoMesaIndex<DomainObject> index2 =
                HBaseGeoMesaIndex.buildDefaultView("securityTest", false, userConn, new DomainObjectValueSerializer());

        final Iterable<DomainObject> results2 = index2.query(GeoMesaQuery.include());
        Assert.assertEquals(1, Iterables.size(results2));
    }

    @Test
    public void testCustomView() throws Exception {
        final GeoMesaIndex<DomainObject> index =
                HBaseGeoMesaIndex.buildWithView(
                        "customview",
                        false,
                        userConn,
                        new DomainObjectValueSerializer(),
                        new SimpleFeatureView<DomainObject>() {
                            AttributeTypeBuilder atb = new AttributeTypeBuilder();
                            private List<AttributeDescriptor> attributeDescriptors =
                                    Lists.newArrayList(atb.binding(Integer.class).buildDescriptor("age"));
                            @Override
                            public void populate(SimpleFeature f, DomainObject domainObject, String id, byte[] payload, Geometry geom, Date dtg) {
                                f.setAttribute("age", 50);
                            }

                            @Override
                            public List<AttributeDescriptor> getExtraAttributes() {
                                return attributeDescriptors;
                            }
                        });

        index.insert(
                one.id,
                one,
                gf.createPoint(new Coordinate(-78.0, 38.0)),
                date("2016-01-01T12:15:00.000Z"));
        index.insert(
                two.id,
                two,
                gf.createPoint(new Coordinate(-78.0, 40.0)),
                date("2016-02-01T12:15:00.000Z"));
        index.flush();

        final FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        final GeoMesaQuery q =
                GeoMesaQuery.GeoMesaQueryBuilder.builder()
                        .within(-79.0, 37.0, -77.0, 39.0)
                        .during(date("2016-01-01T00:00:00.000Z"), date("2016-03-01T00:00:00.000Z"))
                        .filter(ff.equals(ff.property("age"), ff.literal(50)))
                        .build();
        final Iterable<DomainObject> results = index.query(q);

        final Iterable<String> ids = Iterables.transform(results, new Function<DomainObject, String>() {
            @Nullable
            @Override
            public String apply(@Nullable DomainObject domainObject) {
                return domainObject.id;
            }
        });
        Assert.assertArrayEquals("Invalid results", new String[] { "1" }, Iterables.toArray(ids, String.class));

        index.delete("1");

        final Iterable<DomainObject> resultsAfterDelete = index.query(q);
        final Iterable<String> idsPostDelete = Iterables.transform(resultsAfterDelete, new Function<DomainObject, String>() {
            @Nullable
            @Override
            public String apply(@Nullable DomainObject domainObject) {
                System.out.println("Got id " + domainObject.id);
                return domainObject.id;
            }
        });
        Assert.assertArrayEquals("Invalid results", new String[] { }, Iterables.toArray(idsPostDelete, String.class));
    }

    /**
     * Utility method to return all of the table names that begin with the given prefix.
     */
    private static SortedSet<String> filterTablesByPrefix(SortedSet<String> tables, String prefix) {
        SortedSet<String> result = new TreeSet<String>();

        for(String table : tables) {
            if (table.startsWith(prefix)) result.add(table);
        }

        return result;
    }

    @Test
    public void testRemoveSchema() throws Exception {
        String featureName = "deleteme";

        // create the index, and insert a few records
        final GeoMesaIndex<DomainObject> index =
                HBaseGeoMesaIndex.buildWithView(
                        featureName,
                        false,
                        userConn,
                        new DomainObjectValueSerializer(),
                        new SimpleFeatureView<DomainObject>() {
                            AttributeTypeBuilder atb = new AttributeTypeBuilder();
                            private List<AttributeDescriptor> attributeDescriptors =
                                    Lists.newArrayList(atb.binding(Integer.class).buildDescriptor("age"));
                            @Override
                            public void populate(SimpleFeature f, DomainObject domainObject, String id, byte[] payload, Geometry geom, Date dtg) {
                                f.setAttribute("age", 50);
                            }

                            @Override
                            public List<AttributeDescriptor> getExtraAttributes() {
                                return attributeDescriptors;
                            }
                        });
        index.insert(
                one.id,
                one,
                gf.createPoint(new Coordinate(-78.0, 38.0)),
                date("2016-01-01T12:15:00.000Z"));
        index.insert(
                two.id,
                two,
                gf.createPoint(new Coordinate(-78.0, 40.0)),
                date("2016-02-01T12:15:00.000Z"));
        index.flush();
        // fetch the underlying Accumulo data store
        HBaseDataStore ds = ((HBaseGeoMesaIndex)index).ds();

        // look up the tables that exist
        SortedSet<String> preTables = filterTablesByPrefix(listTables(ds), featureName);
        Assert.assertFalse("creating a MockAccumulo instance should create at least one table", preTables.isEmpty());

        // require that the function to pre-compute the names of all tables for this feature type is accurate
        scala.collection.Iterator<GeoMesaFeatureIndex<?, ?>> indices =
              ds.manager().indices(ds.getSchema(featureName), IndexMode$.MODULE$.Any()).iterator();
        List<String> expectedTables = new ArrayList<>();
        while (indices.hasNext()) {
            expectedTables.add(indices.next().getTableNames(Option$.MODULE$.empty()).head());
        }
        expectedTables.add(featureName);

        for (String expectedTable : expectedTables) {
            Assert.assertTrue("Every expected table must be created:  " + expectedTable,
                    preTables.contains(expectedTable));
        }
        Assert.assertEquals("The number of expected tables must exist", expectedTables.size(), preTables.size());

        // remove the schema
        index.removeSchema();

        // ensure that all of the tables are now gone
        SortedSet<String> postTables = filterTablesByPrefix(listTables(ds), featureName);
        Assert.assertTrue("removeScheme on a MockAccumulo instance should remove all but one tables",
                postTables.contains(featureName) && postTables.size() == 1);
    }

    private TreeSet<String> listTables(HBaseDataStore ds) throws IOException {
        return new TreeSet<>(Arrays.stream(ds.connection().getAdmin().listTables()).map(h -> h.getTableName().getNameAsString()).collect(Collectors.toList()));
    }
    private Date date(String s) {
        return Date.from(ZonedDateTime.parse(s).toInstant());
    }
}
