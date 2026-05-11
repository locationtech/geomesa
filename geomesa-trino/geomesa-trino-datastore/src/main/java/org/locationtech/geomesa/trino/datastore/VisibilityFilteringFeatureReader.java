/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

/** Look-ahead reader dropping features the predicate rejects. Backstop behind
 *  the is_visible SQL pushdown — normally a pass-through. */
class VisibilityFilteringFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final FeatureReader<SimpleFeatureType, SimpleFeature> delegate;
    private final Predicate<SimpleFeature> predicate;
    private SimpleFeature next;

    VisibilityFilteringFeatureReader(FeatureReader<SimpleFeatureType, SimpleFeature> delegate,
                                     Predicate<SimpleFeature> predicate) {
        this.delegate = delegate;
        this.predicate = predicate;
    }

    /**
     * Returns the feature type of the underlying reader.
     *
     * @return feature type
     */
    @Override
    public SimpleFeatureType getFeatureType() { return delegate.getFeatureType(); }

    /**
     * Whether another accepted feature is available.
     *
     * @return true if another feature passes the predicate
     */
    @Override
    public boolean hasNext() throws IOException {
        while (next == null && delegate.hasNext()) {
            SimpleFeature candidate = delegate.next();
            if (predicate.test(candidate)) {
                next = candidate;
            }
        }
        return next != null;
    }

    /**
     * Returns the next accepted feature.
     *
     * @return the next feature
     */
    @Override
    public SimpleFeature next() throws IOException {
        if (!hasNext()) throw new NoSuchElementException();
        SimpleFeature result = next;
        next = null;
        return result;
    }

    /**
     * Closes the underlying reader.
     */
    @Override
    public void close() throws IOException { delegate.close(); }
}
