/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface PartitionScheme {

    /**
     * Name of this partition scheme
     *
     * @return name
     */
    String getName();

    /**
     * Return the partition in which a SimpleFeature should be stored
     *
     * @param feature simple feature
     * @return partition name
     */
    String getPartition(SimpleFeature feature);

    @Deprecated
    default List<String> getPartitions(Filter filter) {
        // default implementation provides API compatibility but will throw an error if invoked
        throw new AbstractMethodError();
    }

    /**
     * Return a list of modified filters and partitions. Each filter will have been simplified to
     * remove any predicates that are implicitly true for the associated partitions
     *
     * If the filter does not constrain partitions at all, then an empty option will be returned,
     * indicating all partitions must be searched. If the filter excludes all potential partitions,
     * then an empty list of partitions will be returned
     *
     * Note that this operation is based solely on the partition scheme, so may return partitions
     * that do not actually exist in a given storage instance
     *
     * @param filter filter
     * @return list of simplified filters and partitions
     */
    default Optional<List<FilterPartitions>> getFilterPartitions(Filter filter) {
        return Optional.empty();
    }

    /**
     *
     * @return the max depth this partition scheme goes to
     */
    int getMaxDepth();

    /**
     * Are partitions stored as leaves (multiple partitions in a single folder), or does each
     * partition have a unique folder. Using leaf storage can reduce the level of nesting and make
     * file system operations faster in some cases.
     *
     * @return leaf
     */
    boolean isLeafStorage();

    /**
     * Options used to configure this scheme - @see PartitionSchemeFactory
     *
     * @return options
     */
    Map<String, String> getOptions();
}
