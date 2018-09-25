/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.List;
import java.util.Map;

/**
 * Metadata for a FileSystemStorage instance
 */
public interface StorageMetadata {

    /**
     * Root path for the file system storage
     *
     * @return root path
     */
    Path getRoot();

    /**
     * Handle to the file context used for reading/writing to the underlying file system
     *
     * @return file context
     */
    FileContext getFileContext();

    /**
     * The encoding of the underlying data files
     *
     * @return encoding
     */
    String getEncoding();

    /**
     * The schema for SimpleFeatures stored in the file system storage
     *
     * @return schema
     */
    SimpleFeatureType getSchema();

    /**
     * The partition scheme used to partition features for storage and querying
     *
     * @return partition scheme
     */
    PartitionScheme getPartitionScheme();

    /**
     * Get a partition by name
     *
     * @param name partition name
     * @return partition metadata, or null
     */
    PartitionMetadata getPartition(String name);

    /**
     * The named partitions containing data
     *
     * @return partitions
     */
    List<PartitionMetadata> getPartitions();

    /**
     * Add (or update) metadata for a partition
     *
     * @param partition partition
     */
    void addPartition(PartitionMetadata partition);

    /**
     * Remove all or part of the metadata for a partition
     *
     * @param partition partition
     */
    void removePartition(PartitionMetadata partition);

    /**
     * Rewrite metadata in an optimized fashion
     *
     * Care should be taken with this method. Currently, there is no guarantee for correct behavior if
     * multiple threads or storage instances attempt to compact metadata simultaneously.
     */
    void compact();

    /**
     * Force pick up of any external changes
     */
    void reload();
}
