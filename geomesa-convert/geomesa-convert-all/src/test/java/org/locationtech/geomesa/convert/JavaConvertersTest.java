/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geomesa.convert2.SimpleFeatureConverter;
import org.locationtech.geomesa.convert2.interop.SimpleFeatureConverterLoader;
import org.locationtech.geomesa.utils.collection.CloseableIterator;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JavaConvertersTest {

    @Test
    public void testJavaApi() throws IOException {

        String data = "1,hello,45.0,45.0\n2,world,90.0,90.0\nwillfail,hello";

        Config conf = ConfigFactory.parseString(String.join("\n",
            "{",
            "  type         = \"delimited-text\"",
            "  format       = \"DEFAULT\"",
            "  id-field     = \"md5(string2bytes($0))\"",
            "  fields = [",
            "    { name = \"oneup\",    transform = \"$1\" }",
            "    { name = \"phrase\",   transform = \"concat($1, $2)\" }",
            "    { name = \"lat\",      transform = \"$3::double\" }",
            "    { name = \"lon\",      transform = \"$4::double\" }",
            "    { name = \"lit\",      transform = \"'hello'\" }",
            "    { name = \"geom\",     transform = \"point($lat, $lon)\" }",
            "    { name = \"l1\",       transform = \"concat($lit, $lit)\" }",
            "    { name = \"l2\",       transform = \"concat($l1,  $lit)\" }",
            "    { name = \"l3\",       transform = \"concat($l2,  $lit)\" }",
            "  ]",
            "}"
        ));

        SimpleFeatureType sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"));
        List<SimpleFeature> features = new ArrayList<>();

        try (SimpleFeatureConverter converter = SimpleFeatureConverterLoader.load(sft, conf)) {
            InputStream in = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
            EvaluationContext context = converter.createEvaluationContext(Collections.emptyMap());
            try (CloseableIterator<SimpleFeature> iter = converter.process(in, context)) {
                while (iter.hasNext()) {
                    features.add(iter.next());
                }
            }
            Assert.assertEquals(3, context.line());
            Assert.assertEquals(2, context.success().getCount());
            Assert.assertEquals(1, context.failure().getCount());
        }

        Assert.assertEquals(2, features.size());
        Assert.assertEquals("1hello", features.get(0).getAttribute("phrase"));
        Assert.assertEquals("2world", features.get(1).getAttribute("phrase"));
    }
}
