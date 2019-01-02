/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.attribute;

import com.googlecode.cqengine.attribute.SimpleNullableAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class SimpleFeatureAttribute<A> extends SimpleNullableAttribute<SimpleFeature, A> {
    String fieldName;
    int fieldIndex;

    public SimpleFeatureAttribute(Class<A> fieldType, SimpleFeatureType sft, String fieldName) {
        super(SimpleFeature.class, fieldType, fieldName);
        this.fieldName = fieldName;
        this.fieldIndex = sft.indexOf(fieldName);
    }

    @Override
    public A getValue(SimpleFeature object, QueryOptions queryOptions) {
        return (A) object.getAttribute(fieldIndex);
    }
}
