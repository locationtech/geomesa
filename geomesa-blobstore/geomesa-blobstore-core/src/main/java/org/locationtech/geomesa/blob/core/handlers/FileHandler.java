package org.locationtech.geomesa.blob.core.handlers;

import org.opengis.feature.simple.SimpleFeature;

import java.io.File;
import java.util.Map;

public interface FileHandler {
    public Boolean canProcess(File file, Map<String, String> params);

    public SimpleFeature buildSF(File file, Map<String, String> params);
}
