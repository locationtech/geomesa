/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.filters;

import com.google.common.base.Throwables;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.AbstractMap;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.expression.Expression;
import scala.Tuple2;

public class KryoLazyDensityFilter extends FilterBase {

    private String sftString;
    private SimpleFeatureType sft;
    private KryoFeatureSerializer serializer;
    private Hints hints;

    private SimpleFeatureType outputsft = SimpleFeatureTypes.createType("result", "mapkey:string,weight:java.lang.Double");
    private KryoFeatureSerializer output_serializer = new KryoFeatureSerializer(outputsft, SerializationOptions.withoutId());

    private final String ENVELOPE_OPT = "envelope";
    private final String GRID_WIDTH_OPT = "grid_width";
    private final String GRID_HEIGHT_OPT = "grid_height";
    private final String WEIGHT_OPT = "weight";
    private final String GEOM_INDEX = "geom_index";
    private final String WEIGHT_INDEX = "weight_index";

    private int weightIndex = -2;
    private int geomIndex = -1;

    private GridSnap gridSnap = null;
    private Map<String, Object> options = new HashMap<>();

    private final static Logger logger = LoggerFactory.getLogger(KryoLazyDensityFilter.class);

    public void configure(String sftString, Hints hints){
        this.sftString = sftString;
        configureSFT();
        configureOptions(hints);
    }

    public void configure(SimpleFeatureType sft, Hints hints) {
        this.sft = sft;
        this.sftString = SimpleFeatureTypes.encodeType(sft, true);
        configureOptions(hints);
    }

    public KryoLazyDensityFilter(String sftString, Hints hints) {
        this.sftString = sftString;
        configureSFT();
        configureOptions(hints);
    }

    public KryoLazyDensityFilter(String sftString, Map<String, Object> options) {
        this.sftString = sftString;
        configureSFT();
        if (options == null) {
            return;
        }
        this.options = options;

        this.weightIndex = (Integer) options.getOrDefault(WEIGHT_INDEX, -2);
        this.geomIndex = (Integer) options.getOrDefault(GEOM_INDEX, -1);
        Envelope envelope = (Envelope) options.getOrDefault(ENVELOPE_OPT, null);

        if(envelope != null){
            int width = (Integer) options.get(GRID_WIDTH_OPT);
            int height = (Integer) options.get(GRID_HEIGHT_OPT);
            this.gridSnap = new GridSnap(envelope, width, height);
        }
    }

    public KryoLazyDensityFilter(SimpleFeatureType sft, Hints hints) {
        this.sft = sft;
        this.sftString = SimpleFeatureTypes.encodeType(sft, true);
        configureOptions(hints);
    }

    private void configureOptions(Hints hints) {
        geomIndex = getGeomIndex();
        options.put(GEOM_INDEX, geomIndex);

        if (hints == null) {
            return;
        }

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
        options.put(WEIGHT_INDEX, weightIndex);
    }

    private void configureSFT() {
        sft = SimpleFeatureTypes.createType("densitySft", sftString);
        serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId());
    }

    private String getGeomField() {
        GeometryDescriptor gd = sft.getGeometryDescriptor();
        if (gd == null) {
            return null;
        } else {
            return gd.getLocalName();
        }
    }

    private int getGeomIndex() {
        return sft.indexOf(getGeomField());
    }

    private Envelope getDensityEnvelope(Hints hints) {
        Envelope e = (Envelope) hints.get(QueryHints.DENSITY_BBOX());
        return e;
    }

    private int getDensityWidth(Hints hints) {
        int width = (int) hints.get(QueryHints.DENSITY_WIDTH());
        return width;
    }

    private int getDensityHeight(Hints hints) {
        int height = (int) hints.get(QueryHints.DENSITY_HEIGHT());
        return height;
    }

    private String getDensityWeight(Hints hints) {
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
                logger.error(Throwables.getStackTraceAsString(e));
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
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
        return ReturnCode.INCLUDE;
    }

    @Override
    public Cell transformCell(Cell cell) throws IOException {
        byte[] row = CellUtil.cloneRow(cell);
        SimpleFeature sf_old = serializer.deserialize(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
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
        Cell newCell = CellUtil.createCell(row, arr);
        return newCell;
    }

    // TODO: Add static method to compute byte array from SFT and Filter.
    @Override
    public byte[] toByteArray() throws IOException {
        return Bytes.add(getLengthArray(sftString), serializeHashMap(options));
    }

    private byte[] serializeHashMap(Map<String, Object> map) throws IOException{
        ByteArrayOutputStream fis = new ByteArrayOutputStream();
        ObjectOutputStream ois = new ObjectOutputStream(fis);
        ois.writeObject(map);
        byte[] output = fis.toByteArray();
        ois.close();
        fis.close();
        return Bytes.add(Bytes.toBytes(output.length), output);
    }

    private static Map<String, Object> deserializeHashMap(byte[] bytes){
        try {
            ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
            ObjectInputStream in = new ObjectInputStream(byteIn);
            Map<String, Object> map = (Map<String, Object>) in.readObject();
            return map;
        } catch(ClassNotFoundException cnfe){
            logger.error(Throwables.getStackTraceAsString(cnfe));
        } catch(IOException ie){
            logger.error(Throwables.getStackTraceAsString(ie));
        }
        return null;
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
        int sftLen = Bytes.readAsInt(pbBytes, 0, 4);
        String sftString = new String(Bytes.copy(pbBytes, 4, sftLen));

        int hintsLen = Bytes.readAsInt(pbBytes, sftLen + 4, 4);
        Map<String, Object> options = (Map<String, Object>) deserializeHashMap(Bytes.copy(pbBytes, sftLen + 8, hintsLen));

        return new KryoLazyDensityFilter(sftString, options);
    }

    public Tuple2<Double, Double> decodeKey(Tuple2<Integer, Integer> key){
        double x = gridSnap.x(key._1);
        double y = gridSnap.y(key._2);
        return new Tuple2(x, y);
    }
}
