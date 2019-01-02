/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security;

import com.google.common.base.Joiner;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Utilities for accessing and modifying visibility on <tt>SimpleFeature</tt>s.
 */
public class SecurityUtils {

    public static final String FEATURE_VISIBILITY = "geomesa.feature.visibility";

    /**
     * Sets the visibility to the given {@code visibility} expression.
     *
     * @param feature the <tt>SimpleFeature</tt> to add or update visibility
     * @param visibility the visibility expression
     *
     * @return {@code feature}
     */
    public static SimpleFeature setFeatureVisibility(SimpleFeature feature, String visibility) {
        feature.getUserData().put(FEATURE_VISIBILITY, visibility);
        return feature;
    }

    /**
     * Sets the visbility to an expression created by joining the given {@code visibilities} with "&amp;".
     *
     * @param feature the <tt>SimpleFeature</tt> to add or update visibility
     * @param visibilities a set of visbilities that will be and-ed together
     *
     * @return {@code feature}
     */
    public static SimpleFeature setFeatureVisibilities(SimpleFeature feature, String... visibilities) {
        String and = Joiner.on("&").join(visibilities);
        return setFeatureVisibility(feature, and);
    }

    /**
     * @param feature the <tt>SimpleFeature</tt> to get the visibility from
     * @return the visbility from {@code feature} or null if none
     */
    public static String getVisibility(SimpleFeature feature) {
        return (String)feature.getUserData().get(FEATURE_VISIBILITY);
    }


    /**
     * Copy the visibility from {@code source} to {@code dest}.
     *
     * @param source the <tt>SimpleFeature</tt> to get the visiblity from
     * @param dest the <tt>SimpleFeature</tt> to set the visibility on
     * @throws NullPointerException if either argument is null
     */
    public static void copyVisibility(SimpleFeature source, SimpleFeature dest) {
        setFeatureVisibility(dest, getVisibility(source));
    }
}
