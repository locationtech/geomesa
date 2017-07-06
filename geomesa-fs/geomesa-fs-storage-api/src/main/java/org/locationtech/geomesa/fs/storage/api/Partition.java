/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

/**
 * Abstract class defining a partition. At the moment partitions define
 * file endpoints via a subclass that stores data in the leaf nodes of
 * a tree defined by a PartitionSchema
 *
 * In the future a partition will be able to either store data on leaf
 * nodes or store multiple files in the leaf node. Storing multiple files
 * means that we will likely need to do some sort of compaction of files
 * or provide a command to do so.
 *
 * Two partitions are equal if they have the same name.
 *
 * TODO need to figure out partition vs path for readers and writers
 * to handle having partition schemes that store a single file at a node
 * or multiple files.
 */
abstract public class Partition {
    protected final String name;

    protected Partition(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Partition partition = (Partition) o;

        return name != null ? name.equals(partition.name) : partition.name == null;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Partition{" +
                "name='" + name + '\'' +
                '}';
    }
}

