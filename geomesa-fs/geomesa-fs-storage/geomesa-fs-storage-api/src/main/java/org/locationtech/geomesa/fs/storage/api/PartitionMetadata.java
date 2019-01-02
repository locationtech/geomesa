/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.locationtech.jts.geom.Envelope;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class PartitionMetadata {

    private String name;
    private List<String> files;
    private long count;
    private Envelope bounds;

    // suppress default constructor visibility
    private PartitionMetadata() {}

    public PartitionMetadata(String name, List<String> files, long count, Envelope bounds) {
        this.name = name;
        this.files = Collections.unmodifiableList(files);
        this.count = count;
        this.bounds = new Envelope(bounds);
    }

    /**
     * The name of this partition
     *
     * @return name
     */
    public String name() {
        return name;
    }

    /**
     * The file names of any data files stored in the partition
     *
     * @return file names
     */
    public List<String> files() {
        return files;
    }

    /**
     * The number of features in this partition
     *
     * @return feature count
     */
    public long count() {
        return count;
    }

    /**
     * The bounds for the partition
     *
     * @return partition bounds
     */
    public Envelope bounds() {
        return new Envelope(bounds); // copy for defensive purposes since envelope is mutable
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionMetadata that = (PartitionMetadata) o;
        return count == that.count &&
               Objects.equals(name, that.name) &&
               Objects.equals(files, that.files) &&
               Objects.equals(bounds, that.bounds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, files, count, bounds);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PartitionMetadata(name=").append(name);
        sb.append(",count=").append(count);
        sb.append(",bounds=").append(bounds.getMinX()).append(',').append(bounds.getMinY()).append(',')
          .append(bounds.getMaxX()).append(',').append(bounds.getMaxY());
        sb.append(",files=[");
        Iterator<String> iter = files.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next());
            if (iter.hasNext()) {
                sb.append(',');
            }
        }
        return sb.append("])").toString();
    }
}
