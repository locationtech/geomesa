package org.locationtech.geomesa.fs.storage.api;

import org.opengis.feature.simple.SimpleFeature;

/**
 * Created by anthony on 5/28/17.
 */
public interface FileSystemWriter {

    void writeFeature(SimpleFeature f);

    void flush();

    void close();
}
