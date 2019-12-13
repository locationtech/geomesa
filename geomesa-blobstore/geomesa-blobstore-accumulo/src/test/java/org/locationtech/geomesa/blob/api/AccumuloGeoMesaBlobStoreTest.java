/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.api;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams;
import org.locationtech.geomesa.blob.accumulo.AccumuloGeoMesaBlobStore;
import org.opengis.filter.Filter;

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

    private AccumuloGeoMesaBlobStore agbs;

    @Before
    public void before()  throws Exception {
        Map<String, Serializable> testParams = new HashMap<>();
        testParams.put(AccumuloDataStoreParams.InstanceIdParam().key, "mycloud");
        testParams.put(AccumuloDataStoreParams.ZookeepersParam().key, "zoo1:2181,zoo2:2181,zoo3:2181");
        testParams.put(AccumuloDataStoreParams.UserParam().key, "myuser");
        testParams.put(AccumuloDataStoreParams.PasswordParam().key, "mypassword");
        testParams.put(AccumuloDataStoreParams.CatalogParam().key, "geomesaJava");
        testParams.put(AccumuloDataStoreParams.MockParam().key, "true");
        agbs = new AccumuloGeoMesaBlobStore(testParams);
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

            String id = agbs.put(test1, wkt);
            assertNotNull(id);

            Iterator<String> ids = agbs.getIds(Filter.INCLUDE);
            int idCount = 0;
            while (ids.hasNext()) {
                ids.next();
                idCount++;
            }
            assertTrue(idCount >= 1);

            Blob result = agbs.get(id);
            assertEquals(result.getLocalName(), "testFile.txt");

            agbs.delete(id);

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
        new Random().nextBytes(randomArray);

        String id = agbs.put(randomArray, params);
        assertFalse(id.isEmpty());

        Blob result = agbs.get(id);
        assertEquals(result.getLocalName(), "testrandomarray.txt");
        assertArrayEquals(randomArray, result.getPayload());
    }

    @Test
    public void testOtherConstructor() throws Exception {
        final String instance = "mycloud2";
        final String zoo = "zoo1";
        final String user = "myuser";
        final String pass = "mypassword";
        final String table = "geomesatest2";
        final String auths = "";
        final Boolean mock = Boolean.TRUE;
        final AccumuloGeoMesaBlobStore bs = new AccumuloGeoMesaBlobStore(instance, table, zoo, user, pass, auths, mock);

        final Map<String, String> params = new HashMap<>();
        params.put("geom", "POINT (5 5)");
        params.put("filename", "testrandomarray.txt");

        final String id = bs.put("testBytes".getBytes(), params);

        Blob result = bs.get(id);
        assertEquals(result.getLocalName(), "testrandomarray.txt");
        assertArrayEquals("testBytes".getBytes(), result.getPayload());
    }

}
