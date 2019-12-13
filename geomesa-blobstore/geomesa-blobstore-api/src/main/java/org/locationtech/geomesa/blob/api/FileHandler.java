/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.api;

import org.opengis.feature.simple.SimpleFeature;

import java.io.File;
import java.util.Map;

/**
 * An interface to define how to build SimpleFeatures from a File and a map of parameters
 */
public interface FileHandler {
    /**
     * Indicates whether or not the class can handle the file with the associated parameters.
     * @param file   File to Store
     * @param params Map of parameters indicating or hinting how the processing should work.
     * @return       Whether or not this class can handle the given input
     */
    Boolean canProcess(File file, Map<String, String> params);

    /**
     * This method builds a SimpleFeature given the input file.
     * @param file   File to Store
     * @param params Map of parameters indicating or hinting how to processing should work.
     * @return       SimpleFeature indexing the file.  Must contain a unique ID.
     */
    SimpleFeature buildSimpleFeature(File file, Map<String, String> params);
}
