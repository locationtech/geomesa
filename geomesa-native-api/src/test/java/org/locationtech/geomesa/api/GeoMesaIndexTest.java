package org.locationtech.geomesa.api;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.time.ZonedDateTime;
import java.util.Date;

public class GeoMesaIndexTest {

    public static class DomainObject {
        public String id;
        public int intValue = 0;
        public double doubleValue = 0.0;

        public DomainObject(String id, int intValue, double doubleValue) {
            this.id = id;
            this.intValue = intValue;
            this.doubleValue = doubleValue;
        }
    }

    public static class DomainObjectValueSerializer implements ValueSerializer<DomainObject> {
        public static Gson gson = new Gson();
        @Override
        public byte[] toBytes(DomainObject o) {
            return gson.toJson(o).getBytes();
        }

        @Override
        public DomainObject fromBytes(byte[] bytes) {
            return gson.fromJson(new String(bytes), DomainObject.class);
        }
    }

    @Test
    public void testNativeAPI() {
        DomainObject one = new DomainObject("1", 1, 1.0);
        DomainObject two = new DomainObject("2", 2, 2.0);

        GeoMesaIndex<DomainObject> index = AccumuloGeoMesaIndex.build("hello", "zoo1:2181", "mycloud", "myuser", "mypass", true, new DomainObjectValueSerializer());

        GeometryFactory gf = JTSFactoryFinder.getGeometryFactory();

        index.put(
                one,
                gf.createPoint(new Coordinate(-78.0, 38.0)),
                date("2016-01-01T12:15:00.000Z"));
        index.put(
                two,
                gf.createPoint(new Coordinate(-78.0, 40.0)),
                date("2016-02-01T12:15:00.000Z"));

        GeoMesaQuery q =
                GeoMesaQuery.GeoMesaQueryBuilder.builder()
                        .within(-79.0, 37.0, -77.0, 39.0)
                        .during(date("2016-01-01T00:00:00.000Z"), date("2016-03-01T00:00:00.000Z"))
                        .build();
        Iterable<DomainObject> results = index.query(q);

        Iterable<String> ids = Iterables.transform(results, new Function<DomainObject, String>() {
            @Nullable
            @Override
            public String apply(@Nullable DomainObject domainObject) {
                return domainObject.id;
            }
        });
        Assert.assertArrayEquals("Invalid results", new String[] { "1" }, Iterables.toArray(ids, String.class));
    }

    private Date date(String s) {
        return Date.from(ZonedDateTime.parse("2016-01-01T12:15:00.000Z").toInstant());
    }
}
