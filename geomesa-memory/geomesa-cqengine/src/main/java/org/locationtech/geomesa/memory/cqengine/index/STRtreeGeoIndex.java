/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.index;

import com.googlecode.cqengine.attribute.Attribute;
import org.locationtech.geomesa.memory.cqengine.index.param.STRtreeIndexParam;
import org.locationtech.geomesa.utils.index.WrappedSTRtree;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Optional;

public class STRtreeGeoIndex<A extends Geometry, O extends SimpleFeature> extends AbstractGeoIndex<A, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(STRtreeGeoIndex.class);

    public STRtreeGeoIndex(SimpleFeatureType sft, Attribute<O, A> attribute, Optional<STRtreeIndexParam> geoIndexParams) {
        super(sft, attribute);

        geomAttributeIndex = sft.indexOf(attribute.getAttributeName());
        AttributeDescriptor attributeDescriptor = sft.getDescriptor(geomAttributeIndex);

        STRtreeIndexParam stRtreeIndexParam = geoIndexParams.orElse(new STRtreeIndexParam());
        int nodeCapacity = stRtreeIndexParam.getNodeCapacity();
        LOGGER.debug(MessageFormat.format("STR Tree Index in use :nodeCapacity = {0}", nodeCapacity));

        index = new WrappedSTRtree<>(nodeCapacity);
    }
}
