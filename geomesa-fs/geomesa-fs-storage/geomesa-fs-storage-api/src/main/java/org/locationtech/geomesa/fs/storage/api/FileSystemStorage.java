/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.geotools.data.Query;
import org.opengis.feature.simple.SimpleFeatureType;

import java.net.URI;
import java.util.List;

public interface FileSystemStorage {

    URI getRoot();

    List<String> getTypeNames();
    List<SimpleFeatureType> getFeatureTypes();

    SimpleFeatureType getFeatureType(String typeName);
    void createNewFeatureType(SimpleFeatureType sft, PartitionScheme scheme);

    PartitionScheme getPartitionScheme(String typeName);

    List<String> getPartitions(String typeName);
    List<String> getPartitions(String typeName, Query query);

    List<URI> getPaths(String typeName, String partition);

    FileSystemReader getReader(String typeName, List<String> partitions, Query q);
    FileSystemReader getReader(String typeName, List<String> partitions, Query q, int threads);

    FileSystemWriter getWriter(String typeName, String partition);

    void compact(String typeName, String partition);
    void compact(String typeName, String partition, int threads);

    /**
     * Update the metadata for this filesystem - This should leave the metadata in a
     * consistent and correct state. Currently this is not a thread-safe operation should
     * only be invoked by a single thread.
     *
     * @param typeName
     */
    void updateMetadata(String typeName);

    Metadata getMetadata(String typeName);
}
