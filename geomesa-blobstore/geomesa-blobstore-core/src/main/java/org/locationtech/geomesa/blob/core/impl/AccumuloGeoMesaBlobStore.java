/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.blob.core.impl;

import org.geotools.data.Query;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams;
import org.locationtech.geomesa.blob.core.AccumuloBlobStore;
import org.locationtech.geomesa.blob.core.interop.GeoMesaBlobStore;
import org.opengis.filter.Filter;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AccumuloGeoMesaBlobStore implements GeoMesaBlobStore {

    protected AccumuloBlobStore accumuloBlobStore;

    public AccumuloGeoMesaBlobStore(Map<String, Serializable> dataStoreParams) throws Exception {
        AccumuloDataStoreFactory accumuloDataStoreFactory = new AccumuloDataStoreFactory();
        AccumuloDataStore ds = (AccumuloDataStore) accumuloDataStoreFactory.createDataStore(dataStoreParams);
        if (ds == null) {
            throw new Exception("Error initializing AccumuloGeoMesaBlobStore");
        } else {
            accumuloBlobStore = new AccumuloBlobStore(ds);
        }
    }

    public AccumuloGeoMesaBlobStore(String instanceId,
                                    String tableName,
                                    String zookeepers,
                                    String user,
                                    String password,
                                    String auths) throws Exception {
        Map<String, Serializable> dataStoreParams = new HashMap<>();
        dataStoreParams.put(AccumuloDataStoreParams.instanceIdParam().key, instanceId);
        dataStoreParams.put(AccumuloDataStoreParams.tableNameParam().key, tableName);
        dataStoreParams.put(AccumuloDataStoreParams.zookeepersParam().key, zookeepers);
        dataStoreParams.put(AccumuloDataStoreParams.userParam().key, user);
        dataStoreParams.put(AccumuloDataStoreParams.passwordParam().key, password);
        dataStoreParams.put(AccumuloDataStoreParams.authsParam().key, auths);
        new AccumuloGeoMesaBlobStore(dataStoreParams);
    }

    /**
     * Add a File to the blobstore, relying on available FileHandlers to determine ingest
     * @param file   File to ingest
     * @param params Map String to String, see AccumuloBlobStore for keys
     * @return Blob id as a string or null if put failed
     */
    @Override
    public String put(File file, Map<String, String> params) {
        return accumuloBlobStore.put(file, params);
    }

    /**
     * @param bytes  to ingest, bypass FileHandlers to rely on client to set params
     * @param params Map String to String, see AccumuloBlobStore for keys
     * @return Blob id as a string or null if put failed
     */
    @Override
    public String put(byte[] bytes, Map<String, String> params) {
        return accumuloBlobStore.put(bytes, params);
    }

    /**
     * Query BlobStore for Ids by a opengis Filter
     *
     * @param filter Filter used to query blobstore by
     * @return Iterator of blob Ids that can then be downloaded via get
     */
    @Override
    public Iterator<String> getIds(Filter filter) {
        return accumuloBlobStore.getIds(filter);
    }

    /**
     * Query BlobStore for Ids by a GeoTools Query
     *
     * @param query Query used to query blobstore by
     * @return ids satisfied by the query
     */
    @Override
    public Iterator<String> getIds(Query query) {
        return accumuloBlobStore.getIds(query);
    }

    /**
     * Fetches Blob by id
     *
     * @param id String feature Id of the Blob, from getIds functions
     * @return Tuple2 of (blob, filename)
     */
    @Override
    public Map.Entry<String, byte[]> get(String id) {
        return accumuloBlobStore.get(id);
    }

    /**
     * Deletes Blob by id
     *
     * @param id id of the blob to delete
     */
    @Override
    public void delete(String id) {
        accumuloBlobStore.delete(id);
    }

    /**
     * Deletes BlobStore and all stored features
     * drops accumulo tables associated with this BlobStore
     *
     */
    @Override
    public void deleteBlobStore() {
        accumuloBlobStore.deleteBlobStore();
    }
}
