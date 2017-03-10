/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *************************************************************************/

package org.locationtech.geomesa.hbase.client;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.*;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.locationtech.geomesa.hbase.proto.KryoLazyDensityProto.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.*;

public class FilterAggregatingClient {

    /**
     * An RpcController implementation for use here in this endpoint.
     */
    static class FilterAggregatingClientRpcController implements RpcController {
        private String errorText;
        private boolean cancelled = false;
        private boolean failed = false;

        @Override
        public String errorText() {
            return this.errorText;
        }

        @Override
        public boolean failed() {
            return this.failed;
        }

        @Override
        public boolean isCanceled() {
            return this.cancelled;
        }

        @Override
        public void notifyOnCancel(RpcCallback<Object> arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            this.errorText = null;
            this.cancelled = false;
            this.failed = false;
        }

        @Override
        public void setFailed(String errorText) {
            this.failed = true;
            this.errorText = errorText;
        }

        @Override
        public void startCancel() {
            this.cancelled = true;
        }

    }

    /**
     * It gives the combined result of pairs received from different region servers value of a column for a given column family for the
     * given range. In case qualifier is null, a min of all values for the given
     * family is returned.
     *
     * @param table
     * @return HashMap result;
     * @throws Throwable
     */
    public List<Pair> kryoLazyDensityFilter(final Table table) throws Throwable {

        final DensityRequest requestArg = DensityRequest.newBuilder().build();
        class KryoLazyDensityFilterCallBack implements Callback<List<Pair>> {

            private HashMap<String, Double> finalResult = new HashMap<String, Double>();

            List<Pair> getResult() {
                List<Pair> list = new ArrayList<Pair>();
                for (Entry e : finalResult.entrySet()) {
                    Pair pair = Pair.newBuilder().setKey((String) e.getKey()).setValue((Double) e.getValue()).build();
                    list.add(pair);
                }

                return list;
            }

            @Override
            public synchronized void update(byte[] region, byte[] row, List<Pair> result) {
                for (Pair pair : result) {
                    if (finalResult.containsKey(pair.getKey())) {
                        finalResult.put(pair.getKey(), finalResult.get(pair.getKey()) + pair.getValue());
                    } else {
                        finalResult.put(pair.getKey(), pair.getValue());
                    }
                }
            }
        }

        KryoLazyDensityFilterCallBack kryoLazyDensityFilterCallBack = new KryoLazyDensityFilterCallBack();
        table.coprocessorService(KryoLazyDensityService.class,null, null,
                new Call<KryoLazyDensityService, List<Pair>>() {
                    @Override
                    public List<Pair> call(KryoLazyDensityService instance) throws IOException {
                        RpcController controller = new FilterAggregatingClientRpcController();
                        BlockingRpcCallback<DensityResponse> rpcCallback =
                                new BlockingRpcCallback<>();
                        instance.getDensity(controller, requestArg, rpcCallback);
                        DensityResponse response = rpcCallback.get();
                        if (controller.failed()) {
                            throw new IOException(controller.errorText());
                        }

                        if (response.getPairsList() != null) {
                            return response.getPairsList();
                        } else {
                            return new ArrayList<Pair>();
                        }
                    }
                }, kryoLazyDensityFilterCallBack);

        return kryoLazyDensityFilterCallBack.getResult();
    }


}
