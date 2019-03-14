/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.api;

import com.google.common.collect.Lists;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import java.util.Date;
import java.util.List;

@Deprecated
public class DefaultSimpleFeatureView<T> implements SimpleFeatureView<T> {

    @Override
    public void populate(SimpleFeature f, T t, String id, byte[] payload, Geometry geom, Date dtg) {
        // do nothing
    }

    @Override
    public List<AttributeDescriptor> getExtraAttributes() {
        return Lists.newArrayList();
    }
}
