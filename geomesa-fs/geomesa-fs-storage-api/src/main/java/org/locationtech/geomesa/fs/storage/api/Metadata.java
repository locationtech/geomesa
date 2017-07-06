/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

public interface Metadata {
    void addPartition(String partition, java.util.List<String> files );
    void addPartitions(java.util.Map<String, java.util.List<String>> partitions);
    java.util.List<String> getPartitions();
    java.util.List<String> getFiles(String partition);
}
