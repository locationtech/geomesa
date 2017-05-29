package org.locationtech.geomesa.fs.storage.api;

import org.geotools.data.Query;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.List;

public interface FileSystemStorage {
    List<SimpleFeatureType> listFeatureTypes();

    void createNewFeatureType(SimpleFeatureType sft);

    FileSystemReader getReader(Query q, String partition);

    FileSystemWriter getWriter(String typeName, String partition);
}
