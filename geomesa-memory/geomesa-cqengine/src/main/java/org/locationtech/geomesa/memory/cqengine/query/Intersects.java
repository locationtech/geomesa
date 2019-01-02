/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.query;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.query.simple.SimpleQuery;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

public class Intersects<O, A extends Geometry> extends SimpleQuery<O, A> {
    private final Attribute<O, A> attribute;
    private final A value;

    public Intersects(Attribute<O, A> attribute, A value) {
        super(attribute);
        this.attribute = attribute;
        this.value = value;
    }

    public Envelope getEnvelope() {
        return value.getEnvelopeInternal();
    }

    @Override
    protected boolean matchesSimpleAttribute(SimpleAttribute<O, A> attribute, O object, QueryOptions queryOptions) {
        A attributeValue = attribute.getValue(object, queryOptions);

        return matchesValue(attributeValue, queryOptions);
    }

    @Override
    protected boolean matchesNonSimpleAttribute(Attribute<O, A> attribute, O object, QueryOptions queryOptions) {
        Iterable<A> attributeValues = attribute.getValues(object, queryOptions);
        for (A attributeValue: attributeValues) {
            if (matchesValue(attributeValue, queryOptions)) {
                return true;
            }
        }

        return false;
    }

    @SuppressWarnings("unused")
    public boolean matchesValue(A aValue, QueryOptions queryOptions){
        return value.intersects(aValue);
    }

    public String toString() {
        return "intersects(" +
                this.attribute.getAttributeName() +
                ", " +
                this.value.toString() +
                ")";
    }

    //NB: This is IDEA's auto-generated equals
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Intersects<?, ?> that = (Intersects<?, ?>) o;

        if (!attribute.equals(that.attribute)) return false;
        return value.equals(that.value);
    }

    @Override
    protected int calcHashCode() {
        int result = attribute.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
