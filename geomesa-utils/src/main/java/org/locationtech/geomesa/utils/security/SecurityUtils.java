/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.utils.security;

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
     * Sets the visbility to an expression created by joining the given {@code visibilities} with "&".
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
