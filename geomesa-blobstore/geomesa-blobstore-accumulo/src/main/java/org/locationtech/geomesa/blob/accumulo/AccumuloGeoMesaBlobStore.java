/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.accumulo;

import org.geotools.data.Query;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams;
import org.locationtech.geomesa.blob.api.Blob;
import org.locationtech.geomesa.blob.api.GeoMesaIndexedBlobStore;
import org.opengis.filter.Filter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AccumuloGeoMesaBlobStore implements GeoMesaIndexedBlobStore {

    protected final GeoMesaAccumuloBlobStore geoMesaAccumuloBlobStore;

    public AccumuloGeoMesaBlobStore(final Map<String, Serializable> dataStoreParams) throws IOException {
        final AccumuloDataStore ds = genNewADS(dataStoreParams);
        if (ds == null) {
            throw new IOException("Error initializing AccumuloGeoMesaBlobStore");
        } else {
            geoMesaAccumuloBlobStore = GeoMesaAccumuloBlobStore.apply(ds);
        }
    }

    public AccumuloGeoMesaBlobStore(final String instanceId,
                                    final String tableName,
                                    final String zookeepers,
                                    final String user,
                                    final String password,
                                    final String auths,
                                    final Boolean useMock) throws IOException {
        this(new HashMap<String, Serializable>() {{
            put(AccumuloDataStoreParams.InstanceIdParam().key, instanceId);
            put(AccumuloDataStoreParams.CatalogParam().key, tableName);
            put(AccumuloDataStoreParams.ZookeepersParam().key, zookeepers);
            put(AccumuloDataStoreParams.UserParam().key, user);
            put(AccumuloDataStoreParams.PasswordParam().key, password);
            put(AccumuloDataStoreParams.AuthsParam().key, auths);
            put(AccumuloDataStoreParams.MockParam().key, useMock.toString());
        }});
    }

    private AccumuloDataStore genNewADS(Map<String, Serializable> dataStoreParams) throws IllegalArgumentException {
        AccumuloDataStoreFactory accumuloDataStoreFactory = new AccumuloDataStoreFactory();
        return accumuloDataStoreFactory.createDataStore(dataStoreParams);
    }

    /**
     * Add a File to the blobstore, relying on available FileHandlers to determine ingest
     * @param file   File to ingest
     * @param params Map String to String, see AccumuloBlobStore for keys
     * @return Blob id as a string or null if put failed
     */
    @Override
    public String put(File file, Map<String, String> params) {
        return geoMesaAccumuloBlobStore.put(file, params);
    }

    /**
     * @param bytes  to ingest, bypass FileHandlers to rely on client to set params
     * @param params Map String to String, see AccumuloBlobStore for keys
     * @return Blob id as a string or null if put failed
     */
    @Override
    public String put(byte[] bytes, Map<String, String> params) {
        return geoMesaAccumuloBlobStore.put(bytes, params);
    }

    /**
     * Query BlobStore for Ids by a opengis Filter
     *
     * @param filter Filter used to query blobstore by
     * @return Iterator of blob Ids that can then be downloaded via get
     */
    @Override
    public Iterator<String> getIds(Filter filter) {
        return geoMesaAccumuloBlobStore.getIds(filter);
    }

    /**
     * Query BlobStore for Ids by a GeoTools Query
     *
     * @param query Query used to query blobstore by
     * @return ids satisfied by the query
     */
    @Override
    public Iterator<String> getIds(Query query) {
        return geoMesaAccumuloBlobStore.getIds(query);
    }

    /**
     * Fetches Blob by id
     *
     * @param id String feature Id of the Blob, from getIds functions
     * @return Map.Entry&lt;String, byte[]&gt; map entry of filename to file bytes
     */
    @Override
    public Blob get(String id) {
        return geoMesaAccumuloBlobStore.get(id);
    }

    /**
     * Deletes Blob by id
     *
     * @param id id of the blob to deleteBlob
     */
    @Override
    public void delete(String id) {
        geoMesaAccumuloBlobStore.delete(id);
    }

    /**
     * Deletes BlobStore and all stored features
     * drops accumulo tables associated with this BlobStore
     *
     */
    @Override
    public void deleteBlobStore() {
        geoMesaAccumuloBlobStore.deleteBlobStore();
    }

    @Override
    public void close() throws IOException {
        geoMesaAccumuloBlobStore.close();
    }
}
