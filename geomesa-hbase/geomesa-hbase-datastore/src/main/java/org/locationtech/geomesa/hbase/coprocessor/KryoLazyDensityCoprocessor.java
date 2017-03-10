/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *************************************************************************/

package org.locationtech.geomesa.hbase.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.locationtech.geomesa.features.interop.SerializationOptions;
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer;
import org.locationtech.geomesa.hbase.filters.KryoLazyDensityFilter;
import org.locationtech.geomesa.hbase.proto.KryoLazyDensityProto.*;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KryoLazyDensityCoprocessor extends KryoLazyDensityService implements Coprocessor, CoprocessorService {

    private RegionCoprocessorEnvironment env;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException { }

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void getDensity(RpcController controller, DensityRequest request, RpcCallback<DensityResponse> done) {
        SimpleFeatureType outputsft = SimpleFeatureTypes.createType("result", "mapkey:string,weight:java.lang.Double");
        KryoFeatureSerializer output_serializer = new KryoFeatureSerializer(outputsft, SerializationOptions.withoutId());
        KryoLazyDensityFilter filter = new KryoLazyDensityFilter();
        Scan scan = new Scan();
        scan.setFilter(filter);
        DensityResponse response = null;
        InternalScanner scanner = null;
        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList();
            boolean hasMore = false;
            Map<Tuple2<Double, Double>, Double> resultMap = new HashMap<>();
            do {
                hasMore = scanner.next(results);
                for (Cell cell : results) {
                    byte[] row  = CellUtil.cloneRow(cell);
                    byte[] encodedSF = CellUtil.cloneValue(cell);
                    SimpleFeature sf = output_serializer.deserialize(encodedSF);
                    String str = (String) sf.getAttribute("mapkey");
                    Tuple2<Integer, Integer> keyTemp = (Tuple2<Integer, Integer>) filter.deserializeParameters(str);
                    Tuple2<Double, Double> key = filter.decodeKey(keyTemp);
                    Double value = (Double) sf.getAttribute("weight");
                    resultMap.put(key, resultMap.getOrDefault(key, 0.0) + value);
                }
                results.clear();
            } while (hasMore);
            List<Pair>  pairs = new ArrayList<>();

            resultMap.forEach((Tuple2<Double, Double> key, Double val) -> {
                Pair pair = Pair.newBuilder().setKey(key.toString()).setValue(val).build();
                pairs.add(pair);
            });

            response = DensityResponse.newBuilder().addAllPairs(pairs).build();
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } catch (ClassNotFoundException cfe) {
            cfe.printStackTrace();
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {
                }
            }
        }
        done.run(response);
    }
}