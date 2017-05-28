package org.locationtech.geomesa.fs.storage.api;

import java.io.Serializable;
import java.util.Map;

public interface FileSystemStorageFactory {
    boolean canProcess(Map<String, Serializable> params);
    FileSystemStorage build(Map<String, Serializable> params);
}
