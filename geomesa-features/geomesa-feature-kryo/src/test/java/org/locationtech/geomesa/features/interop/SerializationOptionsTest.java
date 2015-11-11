package org.locationtech.geomesa.features.interop;

import com.vividsolutions.jts.geom.Point;
import junit.framework.Assert;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Test;
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer;
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
        sfBuilder.set("d", new Date());
        Point point = (Point) WKTUtils.read("POINT(" + 45 + " " + 45 + ")");
        sfBuilder.set("geom", point);

        SimpleFeature sf = sfBuilder.buildFeature("1");
        KryoFeatureSerializer serializer = new KryoFeatureSerializer(sft, SerializationOptions.withUserData());

        byte[] serialized = serializer.write(sf);
        SimpleFeature deserialized = serializer.deserialize(serialized);

        Assert.assertNotNull(deserialized);
        Assert.assertEquals(deserialized.getType(), sf.getType());
        Assert.assertEquals(deserialized.getAttributes(), sf.getAttributes());
    }
}
