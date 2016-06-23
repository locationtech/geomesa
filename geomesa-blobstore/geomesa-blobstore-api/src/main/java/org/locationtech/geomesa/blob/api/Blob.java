package org.locationtech.geomesa.blob.api;


import com.google.common.base.Objects;

public class Blob {

    private String id;
    private String localName;
    private byte[] payload;

    public Blob(){

    }

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
        return Objects.toStringHelper(this)
                .add("id", id)
                .add("localName", localName)
                .toString();
    }
}
