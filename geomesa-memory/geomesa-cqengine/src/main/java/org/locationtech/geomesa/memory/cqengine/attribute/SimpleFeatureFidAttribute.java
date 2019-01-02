/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.attribute;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;
import org.opengis.feature.simple.SimpleFeature;

public class SimpleFeatureFidAttribute extends SimpleAttribute<SimpleFeature, String> {
    public SimpleFeatureFidAttribute() {
        super(SimpleFeature.class, String.class, "id");
    }

    @Override
    public String getValue(SimpleFeature object, QueryOptions queryOptions) {
        return object.getID();
    }
}
