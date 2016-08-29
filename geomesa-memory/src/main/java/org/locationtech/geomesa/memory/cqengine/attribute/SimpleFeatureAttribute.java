/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.memory.cqengine.attribute;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;
import org.opengis.feature.simple.SimpleFeature;

public class SimpleFeatureAttribute<A> extends SimpleAttribute<SimpleFeature, A> {

    String fieldName;

    public SimpleFeatureAttribute(Class<A> fieldType, String fieldName) {
        super(SimpleFeature.class, fieldType, fieldName);

        this.fieldName = fieldName;
    }

    @Override
    public A getValue(SimpleFeature object, QueryOptions queryOptions) {
        return (A) object.getAttribute(fieldName);
    }
}
