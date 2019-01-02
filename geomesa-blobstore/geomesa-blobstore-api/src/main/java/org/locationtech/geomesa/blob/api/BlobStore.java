/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.api;


import java.io.Closeable;

/**
 * An interface to define the behavior of a GeoMesa BlobStore
 * TODO: get working with SPI
 */
public interface BlobStore extends Closeable {

    /**
     * GeoMesa BlobStores must be able to persist a file (bytes) by the id
     * generated via a FileHandler and it must also store the
     * localName (filename) of the blob or file in a way that can be
     * retrieved by just the ID
     * @param id generated ID of blob
     * @param localName filename of blob
     * @param bytes bytes of blob
     */
    void put(String id, String localName, byte[] bytes);


    /**
     * GeoMesa BlobStores must be able to retrieve a Blob and the Blob's
     * filename by just the ID returned via the geotools data store index.
     * @param id Blob Id
     * @return blob
     */
    Blob get(String id);

    /**
     * GeoMesa BlobStores must be able to delete a Blob by id.
     *
     * @param id
     */
    void deleteBlob(String id);

    /**
     * A GeoMesa BlobStore may need to delete itself and all content.
     * This should delete all blobs for a particular
     * BlobStore table
     */
    void deleteBlobStore();

}
