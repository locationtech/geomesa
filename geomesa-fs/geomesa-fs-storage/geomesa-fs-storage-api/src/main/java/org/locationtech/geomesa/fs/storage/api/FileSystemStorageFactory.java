/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.Optional;

public interface FileSystemStorageFactory {

    /**
     * The file encoding used by this factory
     *
     * @return encoding
     */
    String getEncoding();

    /**
     * Attempt to load a storage from the given root path. If there is no compatible storage at the path,
     * will return nothing.
     *
     * @param fc file context
     * @param conf configuration
     * @param root storage root path
     * @return storage, if path is compatible with this factory
     */
    Optional<FileSystemStorage> load(FileContext fc, Configuration conf, Path root);

    /**
     * Create a new storage instance at the given root path, which should be empty
     *
     * @param fc file context
     * @param conf configuration
     * @param root storage root path
     * @param sft simple feature type to store
     * @return the newly created storage
     */
    FileSystemStorage create(FileContext fc, Configuration conf, Path root, SimpleFeatureType sft);
}
