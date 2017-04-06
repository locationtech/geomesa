/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.filters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.features.interop.SerializationOptions;
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature;
import org.locationtech.geomesa.index.iterators.IteratorCache;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JSimpleFeatureFilter extends FilterBase {
    private String sftString;
    protected SimpleFeatureType sft;

    protected org.opengis.filter.Filter filter;
    private String filterString;

    public JSimpleFeatureFilter(String sftString, String filterString) throws Exception {
        this.sftString = sftString;
        this.filterString = filterString;
    }

    private static abstract class AbstractFilter extends FilterBase {
        protected SimpleFeatureType sft;
        protected org.opengis.filter.Filter filter;
        static Logger log = LoggerFactory.getLogger(JSimpleFeatureFilter.class);

        AbstractFilter(String sftString, String filterString) {
            log.debug("Instantiating new JSimpleFeatureFilter, sftString = {}, filterString = {}", sftString, filterString);
            sft = IteratorCache.sft(sftString);
            this.filter = IteratorCache.filter(sftString, filterString);
        }


        @Override
        public Cell transformCell(Cell v) throws IOException {
            return v;
        }
    }

    private static class IncludeFilter extends AbstractFilter {
        IncludeFilter(String sftString, String filter) {
            super(sftString, filter);
        }

        @Override
        public ReturnCode filterKeyValue(Cell v) throws IOException {
            return ReturnCode.INCLUDE;
        }
    }

    private static class CQLFilter extends AbstractFilter {
        KryoBufferSimpleFeature reusable;
        CQLFilter(String sftString, String filter) {
            super(sftString, filter);
            reusable = IteratorCache.serializer(sftString, SerializationOptions.withoutId()).getReusableFeature();
        }

        @Override
        public ReturnCode filterKeyValue(Cell v) throws IOException {
            try {
                log.trace("cell = {}", v);
                reusable.setBuffer(v.getValueArray(), v.getValueOffset(), v.getValueLength());
                log.trace("Evaluating filter against SimpleFeature");
                if (filter.evaluate(reusable)) {
                    return ReturnCode.INCLUDE;
                } else {
                    return ReturnCode.SKIP;
                }
            } catch(Exception e) {
                log.error("Exception thrown while scanning, skipping", e);
                return ReturnCode.SKIP;
            }
        }
    }

    public JSimpleFeatureFilter(SimpleFeatureType sft, org.opengis.filter.Filter filter) {
        this.sft = sft;
        this.filter = filter;
        this.sftString = SimpleFeatureTypes.encodeType(sft, true);
        this.filterString = ECQL.toCQL(filter);
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        return ReturnCode.INCLUDE;
    }

    // TODO: Add static method to compute byte array from SFT and Filter.
    @Override
    public byte[] toByteArray() throws IOException {
        return Bytes.add(getLengthArray(sftString), getLengthArray(filterString));
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
        if (s != null) {
            return s.length();
        } else {
            return 0;
        }
    }

    public static org.apache.hadoop.hbase.filter.Filter parseFrom(final byte [] pbBytes) throws DeserializationException {
        int sftLen =  Bytes.readAsInt(pbBytes, 0, 4);
        String sftString = new String(Bytes.copy(pbBytes, 4, sftLen));

        int filterLen = Bytes.readAsInt(pbBytes, sftLen + 4, 4);
        String filterString = new String(Bytes.copy(pbBytes, sftLen + 8, filterLen));

        try {
            if("".equals(filterString)) {
                return new IncludeFilter(sftString, filterString);
            } else {
                return new CQLFilter(sftString, filterString);
            }
        } catch (Exception e) {
            throw new DeserializationException(e);
        }
    }

}
