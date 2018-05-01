/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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
    private int size;
    private Coordinate[] coordinates;


    @Override
    public int getDimension() {
        return dimension;
    }

    @Override
    public Coordinate getCoordinate(int i) {
//        if (coordinates != null) {
//            return coordinates[i];
//        } else {
            double x = bb.getDouble(bb.position() + i * stride);
            double y = bb.getDouble(bb.position() + i * stride + doubleSize);
            return new Coordinate(x, y);
//        }
    }

    @Override
    public Coordinate getCoordinateCopy(int i) {
        return getCoordinate(i);
    }

    // JNH:  Interface says that only deals with X,Y.
    // CoordinateArraySequence deals with XYZ.
    @Override
    public void getCoordinate(int index, Coordinate coord) {
        coord.x = getX(index);
        coord.y = getY(index);
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
        return size;
        //return (bb.limit() - bb.position()) / (doubleSize * dimension);
    }

    @Override
    public void setOrdinate(int index, int ordinateIndex, double value) {
      bb.putDouble(bb.position() + index*stride + doubleSize * ordinateIndex, value);
    }

    @Override
    public Coordinate[] toCoordinateArray() {
        if (coordinates == null) {
            coordinates = new Coordinate[size];
            for (int i = 0; i < size(); i++ ) {
                coordinates[i] = getCoordinate(i);
            }
        }
        return coordinates;
    }

    @Override
    public Envelope expandEnvelope(Envelope env) {
        for (int i = 0; i < size(); i++ ) {
            env.expandToInclude(getX(i), getY(i));
        }
        return env;
    }

    @Override
    public Object clone() {
        return new ByteBufferCoordinateSequence(this.bb, this.dimension, this.size);
    }

    public ByteBufferCoordinateSequence(ByteBuffer bb, int dimension, int size) {
        this.bb = bb;
        this.dimension = dimension;
        this.stride = dimension*doubleSize;
        this.size = size;
    }
}
