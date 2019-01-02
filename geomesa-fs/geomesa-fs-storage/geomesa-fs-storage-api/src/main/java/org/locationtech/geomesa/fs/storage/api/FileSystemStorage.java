/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.apache.hadoop.fs.Path;
import org.geotools.data.Query;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

import java.util.List;

public interface FileSystemStorage {

    /**
     * The metadata for this storage instance
     *
     * @return metadata
     */
    StorageMetadata getMetadata();

    /**
     * Convenience method that delegates to the metadata
     *
     * @see StorageMetadata#getPartitions()
     *
     * @return partitions
     */
    List<PartitionMetadata> getPartitions();

    /**
     * Convenience method that delegates to the partition scheme
     *
     * @see StorageMetadata#getPartitionScheme()
     * @see PartitionScheme#getPartitions(org.opengis.filter.Filter)
     *
     * @return partitions
     */
    List<PartitionMetadata> getPartitions(Filter filter);

    /**
     * Convenience method that delegates to the partition scheme
     *
     * @see StorageMetadata#getPartitionScheme()
     * @see PartitionScheme#getPartition(SimpleFeature)
     *
     * @param feature simple feature
     * @return partition feature should be written to
     */
    String getPartition(SimpleFeature feature);

    /**
     * Get a writer for a given partition. This method is thread-safe and can be called multiple times,
     * although this can result in multiple data files.
     *
     * @param partition partition
     * @return writer
     */
    FileSystemWriter getWriter(String partition);

    /**
     * Get a reader for a given set of partitions, using a single thread
     *
     * @param partitions partitions
     * @param query query
     * @return reader
     */
    FileSystemReader getReader(List<String> partitions, Query query);

    /**
     * Get a reader for a given set of partitions
     *
     * @param partitions partitions
     * @param query query
     * @param threads suggested threads used for reading data files
     * @return reader
     */
    FileSystemReader getReader(List<String> partitions, Query query, int threads);

    /**
     * Get the full paths to any files contained in the partition
     *
     * @param partition partition
     * @return file paths
     */
    List<Path> getFilePaths(String partition);

    /**
     * Compact a partition - merge multiple data files into a single file, using a single reader thread.
     *
     * Care should be taken with this method. Currently, there is no guarantee for correct behavior if
     * multiple threads or storage instances attempt to compact the same partition simultaneously.
     *
     * @param partition partition
     */
    void compact(String partition);

    /**
     * Compact a partition - merge multiple data files into a single file.
     *
     * Care should be taken with this method. Currently, there is no guarantee for correct behavior if
     * multiple threads or storage instances attempt to compact the same partition simultaneously.
     *
     * @param partition partition
     * @param threads suggested threads used for reading to-be-compacted data files
     */
    void compact(String partition, int threads);
}
