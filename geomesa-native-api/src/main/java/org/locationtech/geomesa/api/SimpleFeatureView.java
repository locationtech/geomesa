/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.api;

import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.classification.InterfaceStability;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import java.util.Date;
import java.util.List;

@InterfaceStability.Unstable
@Deprecated
public interface SimpleFeatureView<T> {

    void populate(SimpleFeature f, T t, String id, byte[] payload, Geometry geom, Date dtg);

    List<AttributeDescriptor> getExtraAttributes();

}
