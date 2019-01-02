/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo;

import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureSource;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.Hints;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DocumentationTest {
  public DataStore createDataStore() throws IOException {
    // build the map of parameters
    Map<String,Serializable> params = new HashMap<>();
    params.put("instanceId", "yourcloud");
    params.put("zookeepers" , "zoo1:2181,zoo2:2181,zoo3:2181");
    params.put("user","youruser");
    params.put("password","yourpwd");
    params.put("auths","A,B,C");
    params.put("tableName","testwrite");

    // fetch the data store from the finder
    return DataStoreFinder.getDataStore(params);
  }

  public FeatureIterator queryTest() throws IOException {
    // assume we have previously stored a feature type we called "Product"
    String featureName = "Product";

    // fetch the data store, and use it to get a feature source
    DataStore dataStore = createDataStore();
    FeatureSource featureSource =
      dataStore.getFeatureSource(featureName);

    // submit the "get everything" query, and accept a results iterator
    return featureSource.getFeatures().features();
  }

  public FeatureIterator queryTestFiltered(String featureName)
    throws IOException, CQLException {

    // fetch the data store, and use it to get a feature source
    DataStore dataStore = createDataStore();
    FeatureSource featureSource =
      dataStore.getFeatureSource(featureName);

    // construct a filter that uses both geography and time
    String bbox = "BBOX(geom, -78.0, 38.2, -77.3, 39.1)";
    String period = "( NOT ( dtg AFTER 2012-12-31T23:59:59Z))" +
                    "AND ( NOT ( dtg BEFORE 2012-01-01T00:00:00Z))";
    Filter cqlFilter = ECQL.toFilter(bbox + " AND " + period);
    Query query = new Query(featureName, cqlFilter);

    // submit the filtered query, and accept a results iterator
    return featureSource.getFeatures(query).features();
  }

  public SimpleFeatureType createFeatureType(DataStore dataStore)
    throws IOException, SchemaException {

    // name of the feature to create
    String featureName = "Product";

    // create the feature-type (a "schema" in GeoTools parlance)
    String featureSchema =
      "NAME:String,SKU:Long,COST:Double,SELL_BY:Date,geom:Point:srid=4326,dtg:Date,dtg_end_time:Date";
    SimpleFeatureType featureType = DataUtilities.createType(featureName, featureSchema);
    featureType.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "dtg");
    dataStore.createSchema(featureType);

    return featureType;
  }

  public SimpleFeature createFeature(String featureName, DataStore dataStore)
    throws IOException, ParseException {

    // fetch the feature-type corresponding to this name
    SimpleFeatureType featureType = dataStore.getSchema(featureName);

    // create the feature
    Object[] noValues = {};
    SimpleFeature newFeature = SimpleFeatureBuilder.build(featureType,
      noValues, "SomeNewProductID");

    // initialize a few fields
    // the first three attributes' names are taken from Constants.TYPE_SPEC
    newFeature.setDefaultGeometry((new WKTReader()).read("POINT(45.0 49.0)"));  // name of attribute is geom
    newFeature.setAttribute("dtg", new Date());
    newFeature.setAttribute("dtg_end_time", new Date());
    newFeature.setAttribute("NAME", "New Product Name");
    newFeature.setAttribute("SKU", "011235813");
    newFeature.setAttribute("COST", 1.23);
    newFeature.setAttribute("SELL_BY", new Date());

    return newFeature;
  }

  public void writeFeature(String featureName, SimpleFeature feature,
                           DataStore dataStore)
    throws Exception {

    // get a feature store
    SimpleFeatureSource featureSource = dataStore.getFeatureSource(featureName);
    if (!(featureSource instanceof SimpleFeatureStore))
      throw new Exception("Could not retrieve feature store");
    SimpleFeatureStore featureStore = (SimpleFeatureStore)featureSource;

    // preserve the ID that we created for this feature
    feature.getUserData().put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

    // write this feature collection to the store
    featureStore.addFeatures(DataUtilities.collection(feature));
  }
}
