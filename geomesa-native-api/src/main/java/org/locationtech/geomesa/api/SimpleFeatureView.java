/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.api;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.classification.InterfaceStability;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.Date;

@InterfaceStability.Unstable
public interface SimpleFeatureView<T> {

    IndexType indexType();

    SimpleFeatureType buildSimpleFeatureType();

    SimpleFeature buildSimpleFeature(T t, Geometry geometry);

    SimpleFeature buildSimpleFeature(T t, Geometry geometry, Date date);
}
