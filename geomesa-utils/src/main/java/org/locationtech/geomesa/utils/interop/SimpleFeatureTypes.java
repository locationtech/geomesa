/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.interop;

import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes$;
import org.opengis.feature.simple.SimpleFeatureType;

public class SimpleFeatureTypes {

    public final static String DEFAULT_DATE_KEY =
            org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs$.MODULE$.DEFAULT_DATE_KEY();

    public static SimpleFeatureType createType(String name, String spec) {
        return SimpleFeatureTypes$.MODULE$.createType(name, spec);
    }

    public static SimpleFeatureType mutable(SimpleFeatureType sft) {
        return SimpleFeatureTypes$.MODULE$.mutable(sft);
    }
}
