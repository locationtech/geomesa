/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.List;
import java.util.Map;

/**
 * Metadata for a FileSystemStorage instance
 */
public interface FileMetadata {

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
     * The number of partitions with data
     *
     * @return count
     */
    int getPartitionCount();

    /**
     * The total number of data files, across all partitions
     *
     * @return count
     */
    int getFileCount();

    /**
     * The named partitions containing data
     *
     * @return partitions
     */
    List<String> getPartitions();

    /**
     * The file names of any data files stored in the partition
     *
     * @param partition partition
     * @return file names
     */
    List<String> getFiles(String partition);

    /**
     * The file names of all data files, keyed by partition name
     *
     * @return map of partition -&gt; file names
     */
    Map<String, List<String>> getPartitionFiles();

    /**
     * Add a metadata entry for a file in the given partition
     *
     * @param partition partition
     * @param file file name
     */
    void addFile(String partition, String file);

    /**
     * Add metadata entries for multiple files in the given partition
     *
     * @param partition partition
     * @param files file names
     */
    void addFiles(String partition, List<String> files);

    /**
     * Add metadata entries for multiple files in multiple partitions
     *
     * @param partitionsToFiles map of partition -&gt; file names
     */
    void addFiles(Map<String, List<String>> partitionsToFiles);

    /**
     * Remove the metadata entry for a file in the given partition
     *
     * @param partition partition
     * @param file file name
     */
    void removeFile(String partition, String file);

    /**
     * Remove metadata entries for multiple files in the given partition
     *
     * @param partition partition
     * @param files file names
     */
    void removeFiles(String partition, List<String> files);

    /**
     * Removes metadata entries for multiple files in multiple partitions
     *
     * @param partitionsToFiles map of partition -&gt; file names
     */
    void removeFiles(Map<String, List<String>> partitionsToFiles);

    /**
     * Replaces multiple metadata entries with a single entry for the given partition.
     *
     * @param partition partition
     * @param files file names to replace (remove)
     * @param replacement file name to add
     */
    void replaceFiles(String partition, List<String> files, String replacement);

    /**
     * Removes any current metadata entries and replaces them with the given partitions and files
     *
     * @param partitionsToFiles map of partition -&gt; file names
     */
    void setFiles(Map<String, List<String>> partitionsToFiles);
}
