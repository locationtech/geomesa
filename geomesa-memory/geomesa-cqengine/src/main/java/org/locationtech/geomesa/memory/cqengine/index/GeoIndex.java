/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.index;

import com.googlecode.cqengine.attribute.Attribute;
import org.locationtech.geomesa.memory.cqengine.index.param.BucketIndexParam;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

@Deprecated
public class GeoIndex<A extends Geometry, O extends SimpleFeature> extends BucketGeoIndex<A, O> {
    public GeoIndex(SimpleFeatureType sft, Attribute<O, A> attribute) {
        super(sft, attribute);
    }

    @Deprecated
    public GeoIndex(SimpleFeatureType sft, Attribute<O, A> attribute, int xBuckets, int yBuckets) {
        super(sft, attribute, new BucketIndexParam(xBuckets, yBuckets));
    }

    @Deprecated
    public static <A extends Geometry, O extends SimpleFeature> GeoIndex<A, O> onAttribute(SimpleFeatureType sft, Attribute<O, A> attribute) {
        return (GeoIndex<A, O>) onAttribute(sft, attribute, 360, 180);
    }

    @Deprecated
    public static <A extends Geometry, O extends SimpleFeature> GeoIndex<A, O> onAttribute(SimpleFeatureType sft, Attribute<O, A> attribute, int xBuckets, int yBuckets) {
        return new GeoIndex<A, O>(sft, attribute, xBuckets, yBuckets);
    }


}
