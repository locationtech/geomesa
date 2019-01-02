/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.util.List;
import java.util.Map;

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

    /**
     * Return a list of partitions that the system needs to query
     * in order to satisfy a filter predicate
     *
     * @param filter filter
     * @return list of partitions that may have results from the filter
     */
    List<String> getPartitions(Filter filter);

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
