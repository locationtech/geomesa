package org.locationtech.geomesa.blob.api;


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
        StringBuilder sb = new StringBuilder();
        sb.append("GeoMesa Blob id: ");
        sb.append(id);
        sb.append(", localname: ");
        sb.append(localName);
        return sb.toString();
    }
}
