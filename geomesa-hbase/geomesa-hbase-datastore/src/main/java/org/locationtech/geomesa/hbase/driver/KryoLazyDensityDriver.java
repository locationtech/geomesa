/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.driver;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.*;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.locationtech.geomesa.hbase.proto.KryoLazyDensityProto.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KryoLazyDensityDriver {

    /**
     * An RpcController implementation for use here in this endpoint.
     */
    static class KryoLazyDensityRpcController implements RpcController {
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
    public List<ByteString> kryoLazyDensityFilter(final Table table, final byte[] filter) throws Throwable {

        final DensityRequest requestArg = DensityRequest.newBuilder().setByteFilter(ByteString.copyFrom(filter)).build();
        class KryoLazyDensityFilterCallBack implements Callback<ByteString> {

            private List<ByteString> finalResult = new ArrayList<>();

            List<ByteString> getResult() {
                List<ByteString> list = new ArrayList<ByteString>();
                for (ByteString s : finalResult) {
                    list.add(s);
                }
                return list;
            }

            @Override
            public synchronized void update(byte[] region, byte[] row, ByteString result) {
                finalResult.add(result);
            }
        }

        KryoLazyDensityFilterCallBack kryoLazyDensityFilterCallBack = new KryoLazyDensityFilterCallBack();
        table.coprocessorService(KryoLazyDensityService.class, null, null,
                new Call<KryoLazyDensityService, ByteString>() {
                    @Override
                    public ByteString call(KryoLazyDensityService instance) throws IOException {
                        RpcController controller = new KryoLazyDensityRpcController();
                        BlockingRpcCallback<DensityResponse> rpcCallback =
                                new BlockingRpcCallback<>();
                        instance.getDensity(controller, requestArg, rpcCallback);
                        DensityResponse response = rpcCallback.get();
                        if (controller.failed()) {
                            throw new IOException(controller.errorText());
                        }

                        return response.getSf();
                    }
                }, kryoLazyDensityFilterCallBack);

        return kryoLazyDensityFilterCallBack.getResult();
    }
}
