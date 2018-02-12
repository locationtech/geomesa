/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.interop;

import com.typesafe.config.Config;
import org.locationtech.geomesa.fs.storage.api.PartitionScheme;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.Serializable;
import java.util.Map;

/*
    Helper Methods to convert between Java and Scala and to provide static access to
    the PartitionScheme Scala object.
 */
public class PartitionSchemeUtils {
    public static void addToSft(SimpleFeatureType sft, PartitionScheme scheme) {
        org.locationtech.geomesa.fs.storage.common.PartitionScheme.addToSft(sft, scheme);
    }

    public static PartitionScheme extractFromSft(SimpleFeatureType sft) {
        return org.locationtech.geomesa.fs.storage.common.PartitionScheme.extractFromSft(sft);
    }

    public static PartitionScheme apply(SimpleFeatureType sft, Map<String, Serializable> dsParams) {
        return org.locationtech.geomesa.fs.storage.common.PartitionScheme.apply(sft, dsParams);
    }

    public static PartitionScheme apply(SimpleFeatureType sft, Config conf) {
        return org.locationtech.geomesa.fs.storage.common.PartitionScheme.apply(sft, conf);
    }

    public static Config toConfig(PartitionScheme scheme) {
        return org.locationtech.geomesa.fs.storage.common.PartitionScheme.toConfig(scheme);
    }

    public static String stringify(String schemeName, Map<String, String> opts) {
        return org.locationtech.geomesa.fs.storage.common.PartitionScheme.stringify(schemeName, opts);
    }

    public static PartitionScheme apply(SimpleFeatureType sft, String conf) {
        return org.locationtech.geomesa.fs.storage.common.PartitionScheme.apply(sft, conf);
    }
}
