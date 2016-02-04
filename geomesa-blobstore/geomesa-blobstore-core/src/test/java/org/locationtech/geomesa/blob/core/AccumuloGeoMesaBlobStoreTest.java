/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.blob.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geomesa.blob.core.impl.AccumuloGeoMesaBlobStore;
import org.opengis.filter.Filter;
import scala.Option;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AccumuloGeoMesaBlobStoreTest {

    AccumuloGeoMesaBlobStore agbs;
    Random rand = new Random();

    @Before
    public void before() {
        Map<String, Serializable> testParams = new HashMap<>();
        testParams.put("instanceId", "mycloud");
        testParams.put("zookeepers", "zoo1:2181,zoo2:2181,zoo3:2181");
        testParams.put("user", "myuser");
        testParams.put("password", "mypassword");
        testParams.put("tableName", "geomesaJava");
        testParams.put("useMock", "true");
        try {
            agbs = new AccumuloGeoMesaBlobStore(testParams);
        } catch(Exception e) {
            System.out.println("Error initializing test geomesa blob store in AccumuloGeoMesaBlobStoreTest.java");
        }
    }

    @Test
    public void testBlobStoreIngestQueryAndDelete() {
        URL file = getClass().getClassLoader().getResource("testFile.txt");
        if (file == null) {
            Assert.fail("testFile.txt not found in classloader resources");
        } else {
            File test1 = new File(file.getFile());
            Map<String, String> wkt = new HashMap<>();
            wkt.put("wkt", "POINT (0 0)");

            Option<String> id = agbs.put(test1, wkt);
            assertTrue(id.isDefined());

            Iterator<String> ids = agbs.getIds(Filter.INCLUDE);
            int idCount = 0;
            while (ids.hasNext()) {
                ids.next();
                idCount++;
            }
            assertTrue(idCount >= 1);

            Tuple2<byte[], String> result = agbs.get(id.get());
            assertEquals(result._2, "testFile.txt");

            agbs.delete(id.get());

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Iterator<String> postDeleteIds = agbs.getIds(Filter.INCLUDE);
            postDeleteIds.next();
            assertFalse(postDeleteIds.hasNext());
        }

    }

    @Test
    public void testBlobStoreIngestAndQueryOfDirectAccess() {
        Map<String, String> params = new HashMap<>();
        params.put("geom", "POINT (0 0)");
        params.put("filename", "testrandomarray.txt");

        byte[] randomArray = new byte[64];
        rand.nextBytes(randomArray);

        String id = agbs.put(randomArray, params);
        assertFalse(id.isEmpty());

        Tuple2<byte[], String> result = agbs.get(id);
        assertEquals(result._2, "testrandomarray.txt");
    }

}
