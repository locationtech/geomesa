/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.attribute;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;
import org.geotools.api.feature.simple.SimpleFeature;

public class SimpleFeatureFidAttribute extends SimpleAttribute<SimpleFeature, String> {
    public SimpleFeatureFidAttribute() {
        super(SimpleFeature.class, String.class, "id");
    }

    @Override
    public String getValue(SimpleFeature object, QueryOptions queryOptions) {
        return object.getID();
    }
}
