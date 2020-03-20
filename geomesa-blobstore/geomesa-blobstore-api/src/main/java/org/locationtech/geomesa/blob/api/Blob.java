/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.api;

import java.util.Arrays;

public class Blob {

    private final String id;
    private final String localName;
    private final byte[] payload;

    public Blob(String newID, String newLocalName, byte[] newPayload) {
        id = newID;
        localName = newLocalName;
        payload = newPayload;
    }

    public String getId() {
        return id;
    }

    public String getLocalName() {
        return localName;
    }

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Blob{" +
               "id='" + id + '\'' +
               ", localName='" + localName + '\'' +
               ", payload=" + Arrays.toString(payload) +
               '}';
    }
}
