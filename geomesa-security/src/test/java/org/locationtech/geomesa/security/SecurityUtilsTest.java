/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class SecurityUtilsTest {

    private static final String TEST_VIS = "admin&user";

    @Test
    public void testSetFeatureVisibility() {
        SimpleFeature f = buildFeature();
        SecurityUtils.setFeatureVisibility(f, TEST_VIS);

        Assert.assertEquals(TEST_VIS, f.getUserData().get(SecurityUtils.FEATURE_VISIBILITY));
    }

    @Test
    public void testSetFeatureVisibilities() {
        SimpleFeature f = buildFeature();
        SecurityUtils.setFeatureVisibilities(f, "admin", "user");

        Assert.assertEquals(TEST_VIS, f.getUserData().get(SecurityUtils.FEATURE_VISIBILITY));
    }

    @Test
    public void testGetFeatureVisibility() {
        SimpleFeature f = buildFeature();
        f.getUserData().put(SecurityUtils.FEATURE_VISIBILITY, TEST_VIS);

        Assert.assertEquals("admin&user", SecurityUtils.getVisibility(f));
    }

    @Test
    public void testGetFeatureVisibilityWhenNone() {
        SimpleFeature f = buildFeature();

        Assert.assertNull(SecurityUtils.getVisibility(f));
    }

    @Test
    public void testCopyVisibility() {
        SimpleFeature src = buildFeature();
        SimpleFeature dest = buildFeature();

        SecurityUtils.setFeatureVisibility(src, "src_vis");

        Assert.assertEquals("src_vis", SecurityUtils.getVisibility(src));
        Assert.assertNull(SecurityUtils.getVisibility(dest));

        SecurityUtils.copyVisibility(src, dest);

        Assert.assertEquals("src_vis", SecurityUtils.getVisibility(dest));
    }

    private SimpleFeature buildFeature() {
        SimpleFeatureType sft = SimpleFeatureTypes.createType("test", "name:String,geom:Point:srid=4326");
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
        builder.addAll(new Object[]{"foo"});
        return builder.buildFeature("1");
    }
}
