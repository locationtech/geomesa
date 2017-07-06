/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

public interface PartitionScheme {

    /**
     * Return the partition in which a SimpleFeature should be stored
     * @param sf
     * @return
     */
    String getPartitionName(SimpleFeature sf);

    /**
     * Return a list of partitions that the system needs to query
     * in order to satisfy a filter predicate
     * @param f
     * @return
     */
    java.util.List<String> getCoveringPartitions(Filter f);

    /**
     *
     * @return the max depth this partition scheme goes to
     */
    int maxDepth();

    String toString();

    PartitionScheme fromString(SimpleFeatureType sft, String s);
}
