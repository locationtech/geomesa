/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.query;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SelfAttribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.query.simple.SimpleQuery;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

/**
 * fallback Query type that passes evaluation off to GeoTools. This cannot
 * make use of indexes, so it should be considered a "last chance" fallback.
 */
public class GeoToolsFilterQuery extends SimpleQuery<SimpleFeature, SimpleFeature> {
    // create an "identity" attribute that just returns the SF (currently not used)
    private final static Attribute<SimpleFeature, SimpleFeature> selfAttribute = new SelfAttribute<>(SimpleFeature.class);

    private final Filter filter;

    public GeoToolsFilterQuery(Filter filter) {
        super(selfAttribute);
        this.filter = filter;
    }

    @Override
    protected boolean matchesSimpleAttribute(SimpleAttribute<SimpleFeature, SimpleFeature> attribute, SimpleFeature object, QueryOptions queryOptions) {
        return filter.evaluate(object);
    }

    @Override
    protected boolean matchesNonSimpleAttribute(Attribute<SimpleFeature, SimpleFeature> attribute, SimpleFeature object, QueryOptions queryOptions) {
        return filter.evaluate(object);
    }

    //NB: This is IDEA's auto-generated equals.
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeoToolsFilterQuery that = (GeoToolsFilterQuery) o;

        return filter != null ? filter.equals(that.filter) : that.filter == null;
    }

    @Override
    protected int calcHashCode() {
        return filter.hashCode();
    }
}
