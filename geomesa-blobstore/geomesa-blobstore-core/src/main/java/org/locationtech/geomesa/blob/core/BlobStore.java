/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.blob.core;


import java.io.Closeable;
import java.util.Map;

public interface BlobStore extends Closeable {

    /**
     * BlobStores must be able to put a file (bytes) by the id
     * generated via a FileHandler and it must also store the
     * localName (filename) of the blob in a way that can be
     * retrieved by just the ID
     * @param bytes
     */
    void put(String id, String localName, byte[] bytes);


    /**
     * BlobStores must be able to retrieve a Blob and the Blob's
     * filename by just the ID returned via the geotools data store index.
     * @param id
     * @return
     */
    Map.Entry<String, byte[]> get(String id);

    /**
     * BlobStores must be able to deleteBlob a given blob by id
     * @param id
     */
    void deleteBlob(String id);

    /**
     * A BlobStore may need to delete itself and all contents
     */
    void deleteBlobStore();

}
