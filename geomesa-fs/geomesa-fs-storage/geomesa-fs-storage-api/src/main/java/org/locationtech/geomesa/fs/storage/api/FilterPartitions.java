/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.opengis.filter.Filter;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Holder for a filter to be applied to a group of partitions
 */
public class FilterPartitions {

    private Filter filter;

    private List<String> partitions;

    private boolean partial;

    /**
     * Constructor
     *
     * @param filter filter
     * @param partitions partitions associated with the filter
     * @param partial do the partitions match the full partition format, or only partially
     */
    public FilterPartitions(Filter filter, List<String> partitions, boolean partial) {
        if (filter == null) {
            throw new NullPointerException("Filter must not be null");
        } else if (partitions == null) {
            throw new NullPointerException("Partitions must not be null");
        }
        this.filter = filter;
        this.partitions = partitions;
        this.partial = partial;
    }

    /**
     * Filter to be applied to the included partitions
     *
     * @return filter
     */
    public Filter filter() {
        return filter;
    }

    /**
     * Partitions that match the filter
     *
     * @return partitions
     */
    public List<String> partitions() {
        return partitions;
    }

    /**
     * Are the partitions full partition paths, or only partial matches
     *
     * @return partial
     */
    public boolean partial() {
        return partial;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FilterPartitions that = (FilterPartitions) o;
        return partial == that.partial &&
               Objects.equals(filter, that.filter) && Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, partitions, partial);
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        for (String partition: partitions) {
            joiner.add(partition);
        }
        return "FilterPartitions(filter=" + filter + ",partial=" + partial + ",partitions=" + joiner + ")";
    }
}
