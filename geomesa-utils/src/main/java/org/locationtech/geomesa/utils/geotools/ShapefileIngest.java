/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools;

import org.geotools.data.DataStore;

/**
 * Utility class to help bridge the Java and Scala code for external consumers
 * of the API.
 */
public class ShapefileIngest {
    public static DataStore ingestShapefile(String shapefileName,
                                       DataStore dataStore,
                                       String featureName) {
        // invoke the Scala code
        return org.locationtech.geomesa.utils.geotools.GeneralShapefileIngest$.MODULE$.shpToDataStore(
                shapefileName, dataStore, featureName);
    }
}
