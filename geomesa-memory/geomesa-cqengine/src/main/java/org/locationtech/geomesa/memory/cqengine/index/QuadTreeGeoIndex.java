/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.index;

import com.googlecode.cqengine.attribute.Attribute;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.locationtech.geomesa.utils.index.WrappedQuadtree;
import org.locationtech.jts.geom.Geometry;

public class QuadTreeGeoIndex<A extends Geometry, O extends SimpleFeature> extends AbstractGeoIndex<A, O> {
    public QuadTreeGeoIndex(SimpleFeatureType sft, Attribute<O, A> attribute) {
        super(sft, attribute, new WrappedQuadtree<O>());
    }
}
