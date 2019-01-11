/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kyro.interop;

import org.locationtech.jts.geom.Point;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geomesa.features.interop.SerializationOptions;
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer;
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer$;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.Date;

public class SerializationOptionsTest {

    /**
     * A test to verify SerializationOptions.withUserData()
     */
    @Test
    public void testSerializationOptions() {
        String spec = "a:Integer,b:Double,c:String,dtg:Date,*geom:Point:srid=4326";
        SimpleFeatureType sft = SimpleFeatureTypes.createType("testType", spec);
        SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(sft);

        sfBuilder.set("a", 1);
        sfBuilder.set("b", 2.0);
        sfBuilder.set("c", "foo");
        sfBuilder.set("dtg", new Date());
        Point point = (Point) WKTUtils.read("POINT(45 45)");
        sfBuilder.set("geom", point);

        SimpleFeature sf = sfBuilder.buildFeature("1");
        sf.getUserData().put("TESTKEY", "TESTVAL");
        KryoFeatureSerializer serializer = KryoFeatureSerializer$.MODULE$.apply(sft, SerializationOptions.withUserData());

        byte[] serialized = serializer.serialize(sf);
        SimpleFeature deserialized = serializer.deserialize(serialized);

        Assert.assertNotNull(deserialized);
        Assert.assertEquals(deserialized.getType(), sf.getType());
        Assert.assertEquals(deserialized.getAttributes(), sf.getAttributes());
        Assert.assertEquals(deserialized.getUserData().get("TESTKEY"), "TESTVAL");
    }
}
