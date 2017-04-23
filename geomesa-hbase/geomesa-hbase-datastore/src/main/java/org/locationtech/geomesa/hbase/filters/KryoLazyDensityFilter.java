/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.filters;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.factory.Hints;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.util.Converters;
import org.locationtech.geomesa.features.ScalaSimpleFeature;
import org.locationtech.geomesa.features.interop.SerializationOptions;
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer;
import org.locationtech.geomesa.index.conf.QueryHints;
import org.locationtech.geomesa.utils.geometry.Geometry;
import org.locationtech.geomesa.utils.geotools.GridSnap;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;
import java.util.AbstractMap;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.expression.Expression;
import scala.Tuple2;

public class KryoLazyDensityFilter extends FilterBase {

    private static String sftString;
    private static SimpleFeatureType sft;
    private static KryoFeatureSerializer serializer;
    private static Hints hints;

    private SimpleFeatureType outputsft = SimpleFeatureTypes.createType("result", "mapkey:string,weight:java.lang.Double");
    private KryoFeatureSerializer output_serializer = new KryoFeatureSerializer(outputsft, SerializationOptions.withoutId());

    private static String ENVELOPE_OPT = "envelope";
    private static String GRID_WIDTH_OPT = "grid_width";
    private static String GRID_HEIGHT_OPT = "grid_height";
    private static String WEIGHT_OPT = "weight";

    private static int weightIndex = -2;
    private static int geomIndex = -1;

    private static GridSnap gridSnap = null;
    private static Map<String, Object> options = new HashMap<>();

    public static void configure(String sftString, Hints hints){
        KryoLazyDensityFilter.sftString = sftString;
        configureSFT();
        configureOptions(hints);
    }

    public static void configure(SimpleFeatureType sft, Hints hints) {
        KryoLazyDensityFilter.sft = sft;
        KryoLazyDensityFilter.sftString = SimpleFeatureTypes.encodeType(sft, true);
        configureOptions(hints);
    }
    /*
    TO be used in the coprocessor, default constructor.
     */
    public KryoLazyDensityFilter(){

    }

    public KryoLazyDensityFilter(String sftString, Hints hints) {
        this.sftString = sftString;
        configureSFT();
        configureOptions(hints);
    }

    public KryoLazyDensityFilter(SimpleFeatureType sft, Hints hints) {
        this.sft = sft;
        this.sftString = SimpleFeatureTypes.encodeType(sft, true);
        configureOptions(hints);
    }

    private static void configureOptions(Hints hints) {
        geomIndex = getGeomIndex();
        if (hints == null) return;
        Envelope envelope = getDensityEnvelope(hints);
        int width = getDensityWidth(hints);
        int height = getDensityHeight(hints);
        String weight = getDensityWeight(hints);
        options.put(WEIGHT_OPT, weight);
        options.put(ENVELOPE_OPT, envelope);
        options.put(GRID_WIDTH_OPT, width);
        options.put(GRID_HEIGHT_OPT, height);
        gridSnap = new GridSnap(envelope, width, height);
        int index = sft.indexOf((String) options.get(WEIGHT_OPT));
        weightIndex = index;
    }

    private static void configureSFT() {
        sft = SimpleFeatureTypes.createType("densitySft", sftString);
        serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId());
    }

    private static String getGeomField() {
        GeometryDescriptor gd = sft.getGeometryDescriptor();
        if (gd == null) {
            return null;
        } else {
            return gd.getLocalName();
        }
    }

    private static int getGeomIndex() {
        return sft.indexOf(getGeomField());
    }

    private static Envelope getDensityEnvelope(Hints hints) {
        Envelope e = (Envelope) hints.get(QueryHints.DENSITY_BBOX());
        return e;
    }

    private static int getDensityWidth(Hints hints) {
        int width = (int) hints.get(QueryHints.DENSITY_WIDTH());
        return width;
    }

    private static int getDensityHeight(Hints hints) {
        int height = (int) hints.get(QueryHints.DENSITY_HEIGHT());
        return height;
    }

    private static String getDensityWeight(Hints hints) {
        String weight = (String) hints.get(QueryHints.DENSITY_WEIGHT());
        return weight;
    }


    /**
     * Gets the weight for a feature from a double attribute
     */
    private double getWeightFromDouble(int i, SimpleFeature sf) {
        Double d = (Double) sf.getAttribute(i);
        if (d == null) {
            return 0.0;
        } else {
            return d;
        }
    }

    /**
     * Tries to convert a non-double attribute into a double
     */
    private double getWeightFromNonDouble(int i, SimpleFeature sf) {
        Object d = sf.getAttribute(i);
        if (d == null) {
            return 0.0;
        } else {
            Double converted = Converters.convert(d, Double.class);
            if (converted == null) {
                return 1.0;
            } else {
                return converted;
            }
        }
    }

    /**
     * Evaluates an arbitrary expression against the simple feature to return a weight
     */
    private double getWeightFromExpression(Expression e, SimpleFeature sf) {
        Double d = e.evaluate(sf, Double.class);
        if (d == null) {
            return 0.0;
        } else {
            return d;
        }
    }

    /*
     * Returns the weight of a simple feature
     */
    double weightFn(SimpleFeature sf) {
        double returnVal = 0.0;
        if (weightIndex == -2) {
            returnVal = 1.0;
        } else if (weightIndex == -1) {
            try {
                Expression expression = ECQL.toExpression((String) options.get(WEIGHT_OPT));
                returnVal = getWeightFromExpression(expression, sf);
            } catch (Exception e) {
                System.out.println("Exception");
            }
        } else if (sft.getDescriptor(weightIndex).getType().getBinding() == Double.class) {
            returnVal = getWeightFromDouble(weightIndex, sf);
        } else {
            returnVal = getWeightFromNonDouble(weightIndex, sf);
        }
        return returnVal;
    }

    boolean isPoints() {
        GeometryDescriptor gd = sft.getGeometryDescriptor();
        return (gd != null && gd.getType().getBinding() == Point.class);
    }

    Map.Entry<Tuple2<Integer, Integer>, Double> writePoint(SimpleFeature sf, Double weight) {
        return writePointToResult((Point) sf.getAttribute(geomIndex), weight);
    }

    private Map.Entry<Tuple2<Integer, Integer>, Double> writeNonPoint(Geometry geom, Double weight) {
        return writePointToResult(safeCentroid(geom), weight);
    }

    private Point safeCentroid(Geometry geom) {
        Polygon p = geom.noPolygon();
        Point centroid = p.getCentroid();
        if (Double.isNaN(centroid.getCoordinate().x) || Double.isNaN(centroid.getCoordinate().y)) {
            return p.getEnvelope().getCentroid();
        } else {
            return centroid;
        }
    }

    private Map.Entry<Tuple2<Integer, Integer>, Double> writePointToResult(Point pt, Double weight) {
        return writeSnappedPoint(gridSnap.i(pt.getX()), gridSnap.j(pt.getY()), weight);
    }

    private Map.Entry<Tuple2<Integer, Integer>, Double> writeSnappedPoint(int x, int y, Double weight) {
        Tuple2 tuple = new Tuple2(x, y);
        return new AbstractMap.SimpleEntry(tuple, weight);

    }

    private void writePointToResult(Coordinate pt, Double weight) {
        writeSnappedPoint(gridSnap.i(pt.x), gridSnap.j(pt.y), weight);
    }

    private void writePointToResult(double x, double y, double weight) {
        writeSnappedPoint(gridSnap.i(x), gridSnap.j(y), weight);
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        byte[] encodedSF = CellUtil.cloneValue(v);
        SimpleFeature sf = serializer.deserialize(encodedSF);
        return ReturnCode.INCLUDE;
    }

    @Override
    public Cell transformCell(Cell v) throws IOException {
        byte[] row = CellUtil.cloneRow(v);
        byte[] encodedSF = CellUtil.cloneValue(v);
        SimpleFeature sf_old = serializer.deserialize(encodedSF);
        Map.Entry<Tuple2<Integer, Integer>, Double> out;
        if (isPoints()) {
            out = writePoint(sf_old, weightFn(sf_old));
        } else {
            out = writeNonPoint((Geometry) sf_old.getDefaultGeometry(), weightFn(sf_old));
        }
        SimpleFeature sf = new ScalaSimpleFeature(sf_old.getID(), outputsft, null, null);
        sf.setAttribute(0, serializeParameters(out.getKey()));
        sf.setAttribute(1, out.getValue());
        byte[] arr = output_serializer.serialize(sf);
        Cell cell = CellUtil.createCell(row, arr);
        return cell;
    }

    // TODO: Add static method to compute byte array from SFT and Filter.
    @Override
    public byte[] toByteArray() throws IOException {
//        System.out.println("Serializing JLazyDensityFiler!");
        return Bytes.add(getLengthArray(sftString), convertToBytes(hints));
    }

    private byte[] convertToBytes(Object object) throws IOException {
        if (object == null) {
            return Bytes.toBytes(0);
        }
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(object);
            byte[] arr = bos.toByteArray();
            return Bytes.add(Bytes.toBytes(arr.length), arr);
        }
    }

    private static Object convertFromBytes(byte[] bytes) {
        if (bytes.length == 0) {
            return null;
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInput in = new ObjectInputStream(bis)) {
            return in.readObject();
        } catch (Exception e) {
            return null;
        }
    }

    public Object deserializeParameters(String s) throws IOException,
            ClassNotFoundException {
        byte[] data = Base64.getDecoder().decode(s);
        ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(data));
        Object o = ois.readObject();
        ois.close();
        return o;
    }

    public static String serializeParameters(Serializable o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    private byte[] getLengthArray(String s) {
        int len = getLen(s);
        if (len == 0) {
            return Bytes.toBytes(0);
        } else {
            return Bytes.add(Bytes.toBytes(len), s.getBytes());
        }
    }

    private int getLen(String s) {
        if (s != null) return s.length();
        else return 0;
    }

    public static org.apache.hadoop.hbase.filter.Filter parseFrom(final byte[] pbBytes) throws DeserializationException {
        System.out.println("Creating JdensityFilter with parseFrom!");

        int sftLen = Bytes.readAsInt(pbBytes, 0, 4);
        String sftString = new String(Bytes.copy(pbBytes, 4, sftLen));

        int hintsLen = Bytes.readAsInt(pbBytes, sftLen + 4, 4);
        Hints hints = (Hints) convertFromBytes(Bytes.copy(pbBytes, sftLen + 8, hintsLen));

        return new KryoLazyDensityFilter(sftString, hints);
    }

    public  Tuple2<Double, Double> decodeKey(Tuple2<Integer, Integer> key){
        double x = gridSnap.x(key._1);
        double y = gridSnap.y(key._2);
        return new Tuple2(x, y);
    }
}
