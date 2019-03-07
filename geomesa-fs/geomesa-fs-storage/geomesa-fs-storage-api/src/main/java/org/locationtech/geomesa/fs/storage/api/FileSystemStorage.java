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

import java.util.ArrayList;
import java.util.Collections;
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
    default List<PartitionMetadata> getPartitions() {
        return getMetadata().getPartitions();
    }

    /**
     * Gets a list of partitions that match the given filter
     *
     * @return partitions
     */
    default List<PartitionMetadata> getPartitions(Filter filter) {
        if (filter == Filter.INCLUDE) {
            return getMetadata().getPartitions();
        }

        List<FilterPartitions> fps = getMetadata().getPartitionScheme().getFilterPartitions(filter).orElse(null);

        if (fps == null) {
            return getMetadata().getPartitions();
        }

        List<PartitionMetadata> all = null; // only load if we need to in order to match partial paths
        List<PartitionMetadata> result = new ArrayList<>();

        for (FilterPartitions fp: fps) {
            if (fp.partial()) {
                if (all == null) {
                    all = getMetadata().getPartitions();
                }
                for (PartitionMetadata metadata: all) {
                    if (!result.contains(metadata)) {
                        for (String partition: fp.partitions()) {
                            if (metadata.name().startsWith(partition)) {
                                result.add(metadata);
                                break;
                            }
                        }
                    }
                }
            } else {
                for (String partition: fp.partitions()) {
                    PartitionMetadata metadata = getMetadata().getPartition(partition);
                    if (metadata != null) {
                        result.add(metadata);
                    }
                }
            }

        }
        return result;
    }

    /**
     * Get partitions that match a given filter. Each set of partitions will have a simplified
     * filter that should be applied to that set
     *
     * If there are no partitions that match the filter, an empty list will be returned
     *
     * @return partitions and predicates for each partition
     */
    default List<FilterPartitions> getPartitionsForQuery(Filter filter) {

        List<FilterPartitions> fps = getMetadata().getPartitionScheme().getFilterPartitions(filter).orElse(null);
        List<String> all = null; // only load if we need to in order to match partial paths

        if (fps == null) {
            List<PartitionMetadata> metadata = getMetadata().getPartitions();
            all = new java.util.ArrayList<>(metadata.size());
            for (PartitionMetadata partition: metadata) {
                all.add(partition.name());
            }
            return Collections.singletonList(new FilterPartitions(filter, all, false));
        }

        List<FilterPartitions> result = new ArrayList<>();

        for (FilterPartitions fp: fps) {
            List<String> partitions = new ArrayList<>();
            if (fp.partial()) {
                if (all == null) {
                    List<PartitionMetadata> metadata = getMetadata().getPartitions();
                    all = new ArrayList<>(metadata.size());
                    for (PartitionMetadata partition: metadata) {
                        all.add(partition.name());
                    }
                }
                for (String name: all) {
                    if (!partitions.contains(name)) {
                        for (String partition: fp.partitions()) {
                            if (name.startsWith(partition)) {
                                partitions.add(name);
                                break;
                            }
                        }
                    }
                }
            } else {
                for (String partition: fp.partitions()) {
                    if (getMetadata().getPartition(partition) != null) {
                        partitions.add(partition);
                    }
                }
            }
            if (!partitions.isEmpty()) {
                result.add(new FilterPartitions(fp.filter(), partitions, false));
            }
        }
        return result;
    }

    /**
     * Convenience method that delegates to the partition scheme
     *
     * @see StorageMetadata#getPartitionScheme()
     * @see PartitionScheme#getPartition(SimpleFeature)
     *
     * @param feature simple feature
     * @return partition feature should be written to
     */
    default String getPartition(SimpleFeature feature) {
        return getMetadata().getPartitionScheme().getPartition(feature);
    }

    /**
     * Get a writer for a given partition. This method is thread-safe and can be called multiple times,
     * although this can result in multiple data files.
     *
     * @param partition partition
     * @return writer
     */
    FileSystemWriter getWriter(String partition);

    /**
     * Get a reader for all relevant partitions
     *
     * @param query query
     * @return reader
     */
    default FileSystemReader getReader(Query query) {
        return getReader(query, 1);
    }

    /**
     * Get a reader for all relevant partitions
     *
     * @param query query
     * @param threads suggested threads used for reading data files
     * @return reader
     */
    default FileSystemReader getReader(Query query, int threads) {
        // default implementation provides API compatibility but will throw an error if invoked
        throw new AbstractMethodError();
    }

    /**
     * Get a reader for a single partition
     *
     * @param query query
     * @param partition partition to read
     * @return reader
     */
    default FileSystemReader getPartitionReader(Query query, String partition) {
        return getPartitionReader(query, partition, 1);
    }

    /**
     * Get a reader for a single partition
     *
     * @param query query
     * @param partition partition to read
     * @param threads suggested threads used for reading data files
     * @return reader
     */
    default FileSystemReader getPartitionReader(Query query, String partition, int threads) {
        // default implementation provides API compatibility but will throw an error if invoked
        throw new AbstractMethodError();
    }

    /**
     * Get a reader for a given set of partitions, using a single thread
     *
     * @param partitions partitions
     * @param query query
     * @return reader
     * @deprecated use getPartitionReader(Query, partition)
     */
    @Deprecated
    default FileSystemReader getReader(List<String> partitions, Query query) {
        return getReader(partitions, query, 1);
    }

    /**
     * Get a reader for a given set of partitions
     *
     * @param partitions partitions
     * @param query query
     * @param threads suggested threads used for reading data files
     * @return reader
     * @deprecated use getPartitionReader(Query, partition)
     */
    @Deprecated
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
    default void compact(String partition) {
        compact(partition, 1);
    }

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
