package org.locationtech.geomesa.utils.interop;

import com.vividsolutions.jts.geom.Geometry;
import org.locationtech.geomesa.utils.text.WKTUtils$;

public class WKTUtils {
    public static Geometry read(String WKTgeometry) {
        return WKTUtils$.MODULE$.read(WKTgeometry);
    }

    public static String write(Geometry g) {
        return WKTUtils$.MODULE$.write(g);
    }
}
