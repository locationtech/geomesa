/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf;

import org.opengis.feature.simple.SimpleFeatureType;

public interface TableSplitter {

    /**
     * Get splits for a table
     *
     * @param sft simple feature type
     * @param index name of the index being configured
     * @param options splitter options
     * @return split points
     */
    byte[][] getSplits(SimpleFeatureType sft, String index, String options);

    /**
     * Get splits for a partitioned table
     *
     * Default implementation added for API compatibility, but will be removed in future versions
     *
     * @param sft simple feature type
     * @param index name of the index being configured
     * @param partition name of the partition being configured
     * @param options splitter options
     * @return split points
     */
    default byte[][] getSplits(SimpleFeatureType sft, String index, String partition, String options) {
        // TODO remove default implementation for next major version bump
        return getSplits(sft, index, options);
    }
}
