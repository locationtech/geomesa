/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.interop;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.locationtech.geomesa.utils.text.Suffixes.Memory$;

import java.util.List;

/**
 * Java helper class to set file system storage configuration options in a SimpleFeatureType.
 * <p/>
 * Scala users may prefer the implicits provided by
 * `org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType`
 *
 */
public class ConfigurationUtils {

    public static void setTargetFileSize(SimpleFeatureType sft, String size) {
        Memory$.MODULE$.bytes(size).get(); // validate input
        sft.getUserData().put("geomesa.fs.file-size", size);
    }

    public static void setScheme(SimpleFeatureType sft, String schemes) {
        sft.getUserData().put("geomesa.fs.scheme", schemes);
    }

    public static void setObservers(SimpleFeatureType sft, List<String> observers) {
        sft.getUserData().put("geomesa.fs.observers", String.join(",", observers));
    }
}
