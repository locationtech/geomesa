package org.locationtech.geomesa.jts;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Envelope;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class ByteBufferCoordinateSequence implements CoordinateSequence, Serializable {
    transient private ByteBuffer bb;
    private int dimension;
    private int stride;
    private int doubleSize = 8;


    @Override
    public int getDimension() {
        return dimension;
    }

    @Override
    public Coordinate getCoordinate(int i) {
        double x = bb.getDouble(bb.position() + i*stride);
        double y = bb.getDouble(bb.position() + i*stride+doubleSize);
        return new Coordinate(x, y);
    }

    @Override
    public Coordinate getCoordinateCopy(int i) {
        return null;
    }

    @Override
    public void getCoordinate(int index, Coordinate coord) {
        throw new IllegalArgumentException("Nope");
    }

    @Override
    public double getX(int index) {
        return bb.getDouble(bb.position() + index*stride);
    }

    @Override
    public double getY(int index) {
        return bb.getDouble(bb.position() + index*stride+doubleSize);
    }

    @Override
    public double getOrdinate(int index, int ordinateIndex) {
        return bb.getDouble(bb.position() + index*stride + doubleSize * ordinateIndex);
    }

    @Override
    public int size() {
        return bb.limit() / (doubleSize * dimension);
    }

    @Override
    public void setOrdinate(int index, int ordinateIndex, double value) {

    }

    @Override
    public Coordinate[] toCoordinateArray() {
        throw new IllegalArgumentException("Nope");
        //return new Coordinate[0];
    }

    @Override
    public Envelope expandEnvelope(Envelope env) {
        throw new IllegalArgumentException("Nope");
    }

    @Override
    public Object clone() {
        throw new IllegalArgumentException("Nope");
    }

    public ByteBufferCoordinateSequence(ByteBuffer bb, int dimension) {
        this.bb = bb;
        this.dimension = dimension;
        this.stride = dimension*doubleSize;
    }
}
