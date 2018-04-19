package org.locationtech.geomesa.jts;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequenceFactory;

public class ByteBufferCoordinateSequenceFactory implements CoordinateSequenceFactory {
    @Override
    public CoordinateSequence create(Coordinate[] coordinates) {
        return null;
    }

    @Override
    public CoordinateSequence create(CoordinateSequence coordSeq) {
        return null;
    }

    @Override
    public CoordinateSequence create(int size, int dimension) {
        return null;
    }

    public ByteBufferCoordinateSequenceFactory() {
    }
}
