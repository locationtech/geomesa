/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.index;

import com.googlecode.cqengine.attribute.Attribute;
import org.locationtech.geomesa.memory.cqengine.index.param.BucketIndexParam;
import org.locationtech.geomesa.utils.index.BucketIndex;
import org.locationtech.geomesa.utils.index.SizeSeparatedBucketIndex;
import org.locationtech.geomesa.utils.index.SizeSeparatedBucketIndex$;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Optional;

public class BucketGeoIndex<A extends Geometry, O extends SimpleFeature> extends AbstractGeoIndex<A, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketGeoIndex.class);

    public BucketGeoIndex(SimpleFeatureType sft, Attribute<O, A> attribute, Optional<BucketIndexParam> geoIndexParams) {
        super(sft, attribute);
        AttributeDescriptor attributeDescriptor = sft.getDescriptor(geomAttributeIndex);

        BucketIndexParam params = geoIndexParams.orElse(new BucketIndexParam());

        int xBuckets = params.getxBuckets();
        int yBuckets = params.getyBuckets();
        LOGGER.debug(MessageFormat.format("Bucket Index in use :xBucket = {0}, yBucket={1}", xBuckets, yBuckets));

        if (attributeDescriptor.getType().getBinding() == Point.class) {
            index = new BucketIndex<>(xBuckets, params.getyBuckets(), new Envelope(-180.0, 180.0, -90.0, 90.0));
        } else {
            index = new SizeSeparatedBucketIndex<>(SizeSeparatedBucketIndex$.MODULE$.DefaultTiers(),
                    xBuckets / 360d,
                    yBuckets / 180d,
                    new Envelope(-180.0, 180.0, -90.0, 90.0));
        }
    }
}
