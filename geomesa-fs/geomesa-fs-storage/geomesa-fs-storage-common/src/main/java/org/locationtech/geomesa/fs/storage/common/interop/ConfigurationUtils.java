/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.interop;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.Map;

/**
 * Java helper class to set file system storage configuration options in a SimpleFeatureType.
 *
 * Scala users may prefer the implicits provided by
 * `org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType`
 *
 */
public class ConfigurationUtils {

    private static ConfigRenderOptions renderOptions = ConfigRenderOptions.concise().setFormatted(true);

    public static void setEncoding(SimpleFeatureType sft, String encoding) {
        sft.getUserData().put("geomesa.fs.encoding", encoding);
    }

    public static void setLeafStorage(SimpleFeatureType sft, boolean leafStorage) {
        sft.getUserData().put("geomesa.fs.leaf-storage", String.valueOf(leafStorage));
    }

    public static void setScheme(SimpleFeatureType sft, String scheme, Map<String, String> options) {
        sft.getUserData().put("geomesa.fs.scheme", serialize(scheme, options));
    }

    public static void setMetadata(SimpleFeatureType sft, String name, Map<String, String> options) {
        sft.getUserData().put("geomesa.fs.metadata", serialize(name, options));
    }

    private static String serialize(String name, Map<String, String> options) {
        return ConfigFactory.empty()
                            .withValue("name", ConfigValueFactory.fromAnyRef(name))
                            .withValue("options", ConfigValueFactory.fromMap(options))
                            .root()
                            .render(renderOptions);
    }
}
