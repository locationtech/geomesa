/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.feature.simple.SimpleFeature;
import org.locationtech.geomesa.security.AuthorizationsProvider;
import org.locationtech.geomesa.security.VisibilityUtils;
import scala.Function1;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Client-side visibility backstop. Delegates evaluation to geomesa-security's
 * {@link VisibilityUtils#visible} so the datastore shares the canonical
 * implementation (per-expression caching, invalid expressions hidden /
 * fail-closed) instead of hand-rolling it. Semantics match the plugin's
 * server-side {@code is_visible} UDF: null visibility is unrestricted, and so
 * is the empty string (accumulo-access evaluates an empty expression as
 * accessible to everyone).
 *
 * <p>Constructed from the per-query auths list fetched once in
 * {@code TrinoFeatureSource} — deliberately NOT from the live provider — so
 * this backstop cannot diverge from the extra credential and SQL conjunct sent
 * with the same query. Not thread-safe; create per reader.
 */
class FeatureVisibilityFilter implements Predicate<SimpleFeature> {

    private final Function1<SimpleFeature, Object> delegate;

    FeatureVisibilityFilter(List<String> auths) {
        this.delegate = VisibilityUtils.visible(new AuthorizationsProvider() {
            @Override
            public List<String> getAuthorizations() {
                return auths;
            }

            @Override
            public void configure(Map<String, ?> params) {}
        });
    }

    /**
     * Whether the caller's auths grant access to the feature's visibility.
     *
     * @param feature feature to check
     * @return true if visible (null/empty visibility is unrestricted)
     */
    @Override
    public boolean test(SimpleFeature feature) {
        return (Boolean) delegate.apply(feature);
    }
}
