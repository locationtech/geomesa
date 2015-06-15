/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.interop;

import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes$;
import org.opengis.feature.simple.SimpleFeatureType;

public class SimpleFeatureTypes {

    public static SimpleFeatureType createType(String name, String spec) {
        return SimpleFeatureTypes$.MODULE$.createType(name, spec);
    }
}
