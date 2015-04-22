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
package org.locationtech.geomesa.security;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.hamcrest.CoreMatchers;
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

        Assert.assertThat(f.getUserData().get(SecurityUtils.FEATURE_VISIBILITY), CoreMatchers.equalTo((Object) TEST_VIS));
    }

    @Test
    public void testSetFeatureVisibilities() {
        SimpleFeature f = buildFeature();
        SecurityUtils.setFeatureVisibilities(f, "admin", "user");

        Assert.assertThat(f.getUserData().get(SecurityUtils.FEATURE_VISIBILITY), CoreMatchers.equalTo((Object) TEST_VIS));
    }

    @Test
    public void testGetFeatureVisibility() {
        SimpleFeature f = buildFeature();
        f.getUserData().put(SecurityUtils.FEATURE_VISIBILITY, TEST_VIS);

        Assert.assertThat(SecurityUtils.getVisibility(f), CoreMatchers.equalTo("admin&user"));
    }

    @Test
    public void testGetFeatureVisibilityWhenNone() {
        SimpleFeature f = buildFeature();

        Assert.assertThat(SecurityUtils.getVisibility(f), CoreMatchers.is(CoreMatchers.nullValue()));
    }

    @Test
    public void testCopyVisibility() {
        SimpleFeature src = buildFeature();
        SimpleFeature dest = buildFeature();

        SecurityUtils.setFeatureVisibility(src, "src_vis");

        Assert.assertThat(SecurityUtils.getVisibility(src), CoreMatchers.equalTo("src_vis"));
        Assert.assertThat(SecurityUtils.getVisibility(dest), CoreMatchers.is(CoreMatchers.nullValue()));

        SecurityUtils.copyVisibility(src, dest);

        Assert.assertThat(SecurityUtils.getVisibility(dest), CoreMatchers.equalTo("src_vis"));
    }

    private SimpleFeature buildFeature() {
        SimpleFeatureType sft = SimpleFeatureTypes.createType("test", "name:String,geom:Point:srid=4326");
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
        builder.addAll(new Object[]{"foo"});
        return builder.buildFeature("1");
    }
}
