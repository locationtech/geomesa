package org.locationtech.geomesa.examples;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.cli.*;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.locationtech.geomesa.accumulo.index.Constants;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

public class AccumuloQuickStart {
    static final String INSTANCE_ID = "instanceId";
    static final String ZOOKEEPERS = "zookeepers";
    static final String USER = "user";
    static final String PASSWORD = "password";
    static final String AUTHS = "auths";
    static final String TABLE_NAME = "tableName";

    // sub-set of parameters that are used to create the Accumulo DataStore
    static final String[] ACCUMULO_CONNECTION_PARAMS = new String[] {
        INSTANCE_ID,
        ZOOKEEPERS,
        USER,
        PASSWORD,
        AUTHS,
        TABLE_NAME
    };

    /**
     * Creates a common set of command-line options for the parser.  Each option
     * is described separately.
     */
    static Options getCommonRequiredOptions() {
        Options options = new Options();

        Option instanceIdOpt = OptionBuilder.withArgName(INSTANCE_ID)
                .hasArg()
                .isRequired()
                .withDescription("the ID (name) of the Accumulo instance, e.g:  mycloud")
                .create(INSTANCE_ID);
        options.addOption(instanceIdOpt);

        Option zookeepersOpt = OptionBuilder.withArgName(ZOOKEEPERS)
                .hasArg()
                .isRequired()
                .withDescription("the comma-separated list of Zookeeper nodes that support your Accumulo instance, e.g.:  zoo1:2181,zoo2:2181,zoo3:2181")
                .create(ZOOKEEPERS);
        options.addOption(zookeepersOpt);

        Option userOpt = OptionBuilder.withArgName(USER)
                .hasArg()
                .isRequired()
                .withDescription("the Accumulo user that will own the connection, e.g.:  root")
                .create(USER);
        options.addOption(userOpt);

        Option passwordOpt = OptionBuilder.withArgName(PASSWORD)
                .hasArg()
                .isRequired()
                .withDescription("the password for the Accumulo user that will own the connection, e.g.:  toor")
                .create(PASSWORD);
        options.addOption(passwordOpt);

        Option authsOpt = OptionBuilder.withArgName(AUTHS)
                .hasArg()
                .withDescription("the (optional) list of comma-separated Accumulo authorizations that should be applied to all data written or read by this Accumulo user; note that this is NOT the list of low-level database permissions such as 'Table.READ', but more a series of text tokens that decorate cell data, e.g.:  Accounting,Purchasing,Testing")
                .create(AUTHS);
        options.addOption(authsOpt);

        Option tableNameOpt = OptionBuilder.withArgName(TABLE_NAME)
                .hasArg()
                .isRequired()
                .withDescription("the name of the Accumulo table to use -- or create, if it does not already exist -- to contain the new data")
                .create(TABLE_NAME);
        options.addOption(tableNameOpt);

        return options;
    }

    static Map<String, String> getAccumuloDataStoreConf(CommandLine cmd) {
        Map<String , String> dsConf = new HashMap<String , String>();
        for (String param : ACCUMULO_CONNECTION_PARAMS) {
            dsConf.put(param, cmd.getOptionValue(param));
        }
        if (dsConf.get(AUTHS) == null) dsConf.put(AUTHS, "");
        return dsConf;
    }

    static SimpleFeatureType createSimpleFeatureType(String simpleFeatureTypeName)
            throws SchemaException {

        // list the attributes that constitute the feature type
        List<String> attributes = Lists.newArrayList(
                "Who:String:index=full",
                "What:java.lang.Long",     // some types require full qualification (see DataUtilities docs)
                "When:Date",               // a date-time field is optional, but can be indexed
                "*Where:Point:srid=4326",  // the "*" denotes the default geometry (used for indexing)
                "Why:String"               // you may have as many other attributes as you like...
        );

        // create the bare simple-feature type
        String simpleFeatureTypeSchema = Joiner.on(",").join(attributes);
        SimpleFeatureType simpleFeatureType =
                SimpleFeatureTypes.createType(simpleFeatureTypeName, simpleFeatureTypeSchema);

        // use the user-data (hints) to specify which date-time field is meant to be indexed;
        // if you skip this step, your data will still be stored, it simply won't be indexed
        simpleFeatureType.getUserData().put(Constants.SF_PROPERTY_START_TIME, "When");

        return simpleFeatureType;
    }

    static FeatureCollection createNewFeatures(SimpleFeatureType simpleFeatureType, int numNewFeatures) {
        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();

        String id;
        Object[] NO_VALUES = {};
        String[] PEOPLE_NAMES = {"Addams", "Bierce", "Clemens"};
        Long SECONDS_PER_YEAR = 365L * 24L * 60L * 60L;
        Random random = new Random(5771);
        DateTime MIN_DATE = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"));
        Double MIN_X = -78.0;
        Double MIN_Y = -39.0;
        Double DX = 2.0;
        Double DY = 2.0;

        for (int i = 0; i < numNewFeatures; i ++) {
            // create the new (unique) identifier and empty feature shell
            id = "Observation." + Integer.toString(i);
            SimpleFeature simpleFeature = SimpleFeatureBuilder.build(simpleFeatureType, NO_VALUES, id);

            // be sure to tell GeoTools explicitly that you want to use the ID you provided
            simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

            // populate the new feature's attributes

            // string value
            simpleFeature.setAttribute("Who", PEOPLE_NAMES[i % PEOPLE_NAMES.length]);

            // long value
            simpleFeature.setAttribute("What", i);

            // location:  construct a random point within a 2-degree-per-side square
            double x = MIN_X + random.nextDouble() * DX;
            double y = MIN_Y + random.nextDouble() * DY;
            Geometry geometry = WKTUtils.read("POINT(" + x + " " + y + ")");

            // date-time:  construct a random instant within a year
            simpleFeature.setAttribute("Where", geometry);
            DateTime dateTime = MIN_DATE.plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR));
            simpleFeature.setAttribute("When", dateTime.toDate());

            // another string value
            // "Why"; left empty, showing that not all attributes need values

            // accumulate this new feature in the collection
            featureCollection.add(simpleFeature);
        }

        return featureCollection;
    }

    static void insertFeatures(String simpleFeatureTypeName,
                               DataStore dataStore,
                               FeatureCollection featureCollection)
            throws IOException {

        FeatureStore featureStore = (SimpleFeatureStore) dataStore.getFeatureSource(simpleFeatureTypeName);
        featureStore.addFeatures(featureCollection);
    }

    static Filter createFilter(String geomField, double x0, double y0, double x1, double y1,
                               String dateField, String t0, String t1,
                               String attributesQuery)
            throws CQLException, IOException {

        // there are many different geometric predicates that might be used;
        // here, we just use a bounding-box (BBOX) predicate as an example.
        // this is useful for a rectangular query area
        String cqlGeometry = "BBOX(" + geomField + ", " +
                x0 + ", " + y0 + ", " + x1 + ", " + y1 + ")";

        // there are also quite a few temporal predicates; here, we use a
        // "DURING" predicate, because we have a fixed range of times that
        // we want to query
        String cqlDates = "(" + dateField + " DURING " + t0 + "/" + t1 + ")";

        // there are quite a few predicates that can operate on other attribute
        // types; the GeoTools Filter constant "INCLUDE" is a default that means
        // to accept everything
        String cqlAttributes = attributesQuery == null ? "INCLUDE" : attributesQuery;

        String cql = cqlGeometry + " AND " + cqlDates  + " AND " + cqlAttributes;
        return CQL.toFilter(cql);
    }

    static void queryFeatures(String simpleFeatureTypeName,
                              DataStore dataStore,
                              String geomField, double x0, double y0, double x1, double y1,
                              String dateField, String t0, String t1,
                              String attributesQuery)
            throws CQLException, IOException {

        // construct a (E)CQL filter from the search parameters,
        // and use that as the basis for the query
        Filter cqlFilter = createFilter(geomField, x0, y0, x1, y1, dateField, t0, t1, attributesQuery);
        Query query = new Query(simpleFeatureTypeName, cqlFilter);

        // submit the query, and get back an iterator over matching features
        FeatureSource featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
        FeatureIterator featureItr = featureSource.getFeatures(query).features();

        // loop through all results
        int n = 0;
        while (featureItr.hasNext()) {
            Feature feature = featureItr.next();
            System.out.println((++n) + ".  " +
                    feature.getProperty("Who").getValue() + "|" +
                    feature.getProperty("What").getValue() + "|" +
                    feature.getProperty("When").getValue() + "|" +
                    feature.getProperty("Where").getValue() + "|" +
                    feature.getProperty("Why").getValue());
        }
        featureItr.close();
    }

    static void secondaryIndexExample(String simpleFeatureTypeName,
                                      DataStore dataStore,
                                      String[] attributeFields,
                                      String attributesQuery,
                                      int maxFeatures,
                                      String sortByField)
            throws CQLException, IOException {

        // construct a (E)CQL filter from the search parameters,
        // and use that as the basis for the query
        Filter cqlFilter = CQL.toFilter(attributesQuery);
        Query query = new Query(simpleFeatureTypeName, cqlFilter);

        query.setPropertyNames(attributeFields);
        query.setMaxFeatures(maxFeatures);

        if (!sortByField.equals("")) {
            FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
            SortBy[] sort = new SortBy[]{ff.sort(sortByField, SortOrder.DESCENDING)};
            query.setSortBy(sort);
        }

        // submit the query, and get back an iterator over matching features
        FeatureSource featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
        FeatureIterator featureItr = featureSource.getFeatures(query).features();

        // loop through all results
        int n = 0;
        while (featureItr.hasNext()) {
            Feature feature = featureItr.next();
            StringBuilder sb = new StringBuilder();
            sb.append("Feature ID ").append(feature.getIdentifier());

            for (String field : attributeFields) {
                sb.append(" | ").append(field).append(": ").append(feature.getProperty(field).getValue());
            }
            System.out.println(sb.toString());
        }
        featureItr.close();
    }

    public static void main(String[] args) throws Exception {
        // find out where -- in Accumulo -- the user wants to store data
        CommandLineParser parser = new BasicParser();
        Options options = getCommonRequiredOptions();
        CommandLine cmd = parser.parse( options, args);

        // verify that we can see this Accumulo destination in a GeoTools manner
        Map<String, String> dsConf = getAccumuloDataStoreConf(cmd);
        DataStore dataStore = DataStoreFinder.getDataStore(dsConf);
        assert dataStore != null;

        // establish specifics concerning the SimpleFeatureType to store
        String simpleFeatureTypeName = "AccumuloQuickStart";
        SimpleFeatureType simpleFeatureType = createSimpleFeatureType(simpleFeatureTypeName);

        // write Feature-specific metadata to the destination table in Accumulo
        // (first creating the table if it does not already exist); you only need
        // to create the FeatureType schema the *first* time you write any Features
        // of this type to the table
        System.out.println("Creating feature-type (schema):  " + simpleFeatureTypeName);
        dataStore.createSchema(simpleFeatureType);

        // create new features locally, and add them to this table
        System.out.println("Creating new features");
        FeatureCollection featureCollection = createNewFeatures(simpleFeatureType, 1000);
        System.out.println("Inserting new features");
        insertFeatures(simpleFeatureTypeName, dataStore, featureCollection);

        // query a few Features from this table
        System.out.println("Submitting query");
        queryFeatures(simpleFeatureTypeName, dataStore,
                "Where", -77.5, -37.5, -76.5, -36.5,
                "When", "2014-07-01T00:00:00.000Z", "2014-09-30T23:59:59.999Z",
                "(Who = 'Bierce')");

        System.out.println("Submitting secondary index query");
        secondaryIndexExample(simpleFeatureTypeName, dataStore,
                new String[]{"Who"},
                "(Who = 'Bierce')",
                5,
                "");
        System.out.println("Submitting secondary index query with sorting (sorted by 'What' descending)");
        secondaryIndexExample(simpleFeatureTypeName, dataStore,
                new String[]{"Who", "What"},
                "(Who = 'Addams')",
                5,
                "What");
    }
}
