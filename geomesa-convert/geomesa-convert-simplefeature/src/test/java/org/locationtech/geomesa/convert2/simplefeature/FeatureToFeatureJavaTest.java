package org.locationtech.geomesa.convert2.simplefeature;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geomesa.convert.ConverterConfigLoader;
import org.locationtech.geomesa.convert.EvaluationContext;
import org.locationtech.geomesa.utils.collection.CloseableIterator;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FeatureToFeatureJavaTest {

    @Test
    public void testJavaApi() {
        Config outConfPoint = ConfigFactory.parseString(
                "{ type-name = outtype, attributes = [" +
                        "{ name = number,   type = Integer }," +
                        "{ name = color,    type = String  }," +
                        "{ name = weight,   type = Double  }," +
                        "{ name = numberx2, type = Integer }," +
                        "{ name = geom,     type = Point   }" +
                        "]}");

        Config parserConf = ConfigFactory.parseString(
                "{ type = simple-feature, input-sft = intype, fields = [" +
                        "{ name = number,   transform = \"$number\" }," +
                        "{ name = color ,   transform = \"$color\"  }," +
                        "{ name = weight,   transform = \"$weight\" }," +
                        "{ name = geom,     transform = \"$geom\"   }," +
                        "{ name = numberx2, transform = \"add($number, $number)::int\" }" +
                        "]}");

        SimpleFeatureType insft  = SimpleFeatureTypeLoader.sftForName("intype").get();
        SimpleFeatureType outsft = SimpleFeatureTypes.createType(outConfPoint);
        ConverterConfigLoader.configForName("myconverter");
        FeatureToFeatureConverter converter = FeatureToFeatureConverter.apply(outsft, parserConf);
        Assert.assertNotNull(converter);

        Geometry pt = WKTUtils.read("POINT(0 0)");
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(insft);
        builder.reset();
        builder.addAll(1, "blue", 10.0, pt);
        SimpleFeature sf = builder.buildFeature("1");

        EvaluationContext ec = converter.createEvaluationContext(Map.of());
        try(CloseableIterator<SimpleFeature> res = converter.convert(CloseableIterator.apply(List.of(sf).iterator()), ec)) {
            Assert.assertTrue(res.hasNext());
            SimpleFeature next = res.next();
            Assert.assertFalse(res.hasNext());
            Assert.assertEquals(2, next.getAttribute("numberx2"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
