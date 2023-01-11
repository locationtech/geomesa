/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.interop;

import org.locationtech.geomesa.utils.text.WKTUtils$;
import org.locationtech.jts.geom.Geometry;

public class WKTUtils {
    public static Geometry read(String WKTgeometry) {
        return WKTUtils$.MODULE$.read(WKTgeometry);
    }

    public static String write(Geometry g) {
        return WKTUtils$.MODULE$.write(g);
    }
}
