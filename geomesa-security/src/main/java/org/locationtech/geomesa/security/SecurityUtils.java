/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security;

import org.geotools.api.feature.simple.SimpleFeature;

/**
 * Utilities for accessing and modifying visibility on `SimpleFeature`s.
 */
public class SecurityUtils {

    public static final String FEATURE_VISIBILITY = "geomesa.feature.visibility";

    /**
     * Sets the visibility to the given {@code visibility} expression.
     *
     * @param feature the `SimpleFeature` to add or update visibility
     * @param visibility the visibility expression
     *
     * @return {@code feature}
     */
    public static SimpleFeature setFeatureVisibility(SimpleFeature feature, String visibility) {
        String value = null;
        if (visibility != null) {
            value = visibility.intern();
        }
        feature.getUserData().put(FEATURE_VISIBILITY, value);
        return feature;
    }

    /**
     * Sets the visibility to an expression created by joining the given {@code visibilities} with "&amp;".
     *
     * @param feature the `SimpleFeature` to add or update visibility
     * @param visibilities a set of visibilities that will be and-ed together
     *
     * @return {@code feature}
     */
    public static SimpleFeature setFeatureVisibilities(SimpleFeature feature, String... visibilities) {
        return setFeatureVisibility(feature, String.join("&", visibilities));
    }

    /**
     * @param feature the `SimpleFeature` to get the visibility from
     * @return the visibility from {@code feature} or null if none
     */
    public static String getVisibility(SimpleFeature feature) {
        return (String) feature.getUserData().get(FEATURE_VISIBILITY);
    }


    /**
     * Copy the visibility from {@code source} to {@code dest}.
     *
     * @param source the `SimpleFeature` to get the visibility from
     * @param dest the `SimpleFeature` to set the visibility on
     * @throws NullPointerException if either argument is null
     */
    public static void copyVisibility(SimpleFeature source, SimpleFeature dest) {
        setFeatureVisibility(dest, getVisibility(source));
    }
}
