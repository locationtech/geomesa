/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.api;

import org.geotools.data.Query;
import org.opengis.filter.Filter;

import java.io.Closeable;
import java.io.File;
import java.util.Iterator;
import java.util.Map;

/**
 * An interface to define how users may ingest and query a GeoMesa BlobStore
 */
public interface GeoMesaIndexedBlobStore extends Closeable {

    /**
     * Add a File to the blobstore, relying on available FileHandlers to determine ingest
     * @param file File to ingest
     * @param params Map of parameters - see implementation for details
     * @return Blob id as a string or null if put failed
     */
    String put(File file, Map<String, String> params);

    /**
     * Adds a byte array to the blobstore, relying on user provided params for geo-indexing
     * @param bytes to ingest, bypass FileHandlers to rely on client to set params
     * @param params Map of parameters - see implementation for details
     * @return Blob id as a string or null if put failed
     */
    String put(byte[] bytes, Map<String, String> params);

    /**
     * Query BlobStore for Ids by a opengis Filter
     * @param filter Filter used to query blobstore by
     * @return Iterator of blob Ids that can then be downloaded via get
     */
    Iterator<String> getIds(Filter filter);

    /**
     * Query BlobStore for Ids by a GeoTools Query
     * @param query Query used to query blobstore by
     * @return
     */
    Iterator<String> getIds(Query query);

    /**
     * Fetches Blob by id
     * @param id String feature Id of the Blob, from getIds functions
     * @return Map.Entry&lt;String, byte[]&gt; map entry of filename to file bytes
     */
    Blob get(String id);

    /**
     * Deletes a blob from the blobstore by id
     * @param id id of the blob to deleteBlob
     */
    void delete(String id);

    /**
     * Deletes BlobStore and all stored features
     */
    void deleteBlobStore();

}
