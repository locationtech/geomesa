package org.locationtech.geomesa.jts;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Envelope;

public class ByteBufferCoordinateSequence implements CoordinateSequence {
    @Override
    public int getDimension() {
        return 0;
    }

    @Override
    public Coordinate getCoordinate(int i) {
        return null;
    }

    @Override
    public Coordinate getCoordinateCopy(int i) {
        return null;
    }

    @Override
    public void getCoordinate(int index, Coordinate coord) {

    }

    @Override
    public double getX(int index) {
        return 0;
    }

    @Override
    public double getY(int index) {
        return 0;
    }

    @Override
    public double getOrdinate(int index, int ordinateIndex) {
        return 0;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void setOrdinate(int index, int ordinateIndex, double value) {

    }

    @Override
    public Coordinate[] toCoordinateArray() {
        return new Coordinate[0];
    }

    @Override
    public Envelope expandEnvelope(Envelope env) {
        return null;
    }

    @Override
    public Object clone() {
        return null;
    }

    public ByteBufferCoordinateSequence() {
    }
}
