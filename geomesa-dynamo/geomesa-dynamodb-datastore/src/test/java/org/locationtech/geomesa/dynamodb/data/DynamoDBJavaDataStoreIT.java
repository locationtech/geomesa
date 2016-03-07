/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

public class DynamoDBJavaDataStoreIT {

    @Test
    public void testDataAccess() throws IOException {
        DataStore db = getDataStore();
        assert db != null;
        db.createSchema(SimpleFeatureTypes.createType("test", "testjavaaccess", "foo:Int,dtg:Date,*geom:Point:srid=4326"));
        Assert.assertTrue("Types should contain testjavaaccess", Collections2.filter(Arrays.asList(db.getTypeNames()), Predicates.equalTo("testjavaaccess")).size() == 1);
    }

    public DataStore getDataStore() throws IOException {
        Map<String, ?> params = ImmutableMap.of(
                DynamoDBDataStoreFactory.CATALOG().getName(), String.format("ddbtest%s", UUID.randomUUID().toString()),
                DynamoDBDataStoreFactory.DYNAMODBAPI().getName(), getNewDynamoDB()
        );
        return DataStoreFinder.getDataStore(params);
    }

    public DynamoDB getNewDynamoDB() {
        AmazonDynamoDBAsyncClient adbc = new AmazonDynamoDBAsyncClient(new BasicAWSCredentials("", ""));
        adbc.setEndpoint(String.format("http://localhost:%s", System.getProperty("dynamodb.port")));
        return new DynamoDB(adbc);
    }

}
