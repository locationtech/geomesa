/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.filters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.features.interop.SerializationOptions;
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature;
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer;
import org.locationtech.geomesa.index.iterators.IteratorCache;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JSimpleFeatureFilter extends FilterBase {
    private String sftString;
    protected SimpleFeatureType sft;

    protected org.opengis.filter.Filter filter;
    private String filterString;

    private String transform;
    private String transformSchema;

    private static abstract class AbstractFilter extends FilterBase {
        protected SimpleFeatureType sft;
        protected org.opengis.filter.Filter filter;
        protected Boolean hasTransform;
        protected KryoBufferSimpleFeature reusable;
        static Logger log = LoggerFactory.getLogger(JSimpleFeatureFilter.class);

        AbstractFilter(String sftString, String filterString, String transform, String transformSchema) {
            log.debug("Instantiating new JSimpleFeatureFilter, sftString = {}, filterString = {}", sftString, filterString);
            sft = IteratorCache.sft(sftString);
            this.filter = IteratorCache.filter(sftString, filterString);

            reusable = IteratorCache.serializer(sftString, SerializationOptions.withoutId()).getReusableFeature();
            if(transform == null || transform.isEmpty() || transformSchema == null || transformSchema.isEmpty()) {
                hasTransform = false;
            } else {
                hasTransform = true;
                reusable.setTransforms(transform, SimpleFeatureTypes.createType("", transformSchema));
            }
        }


        @Override
        public Cell transformCell(Cell v) throws IOException {
            if(hasTransform) {
                return CellUtil.createCell(v.getRow(), v.getFamily(), v.getQualifier(), v.getTimestamp(), v.getTypeByte(), reusable.transform());
            } else {
                return super.transformCell(v);
            }
        }
    }

    private static class IncludeFilter extends AbstractFilter {
        IncludeFilter(String sftString, String filter, String transform, String transformSchema) {
            super(sftString, filter, transform, transformSchema);
        }

        @Override
        public ReturnCode filterKeyValue(Cell v) throws IOException {
            return ReturnCode.INCLUDE;
        }
    }

    private static class CQLFilter extends AbstractFilter {
        CQLFilter(String sftString, String filter, String transform, String transformSchema) {
            super(sftString, filter, transform, transformSchema);
        }

        @Override
        public ReturnCode filterKeyValue(Cell v) throws IOException {
            try {
                log.trace("cell = {}", v);
                if (hasTransform) {
                    reusable.setBuffer(CellUtil.cloneValue(v));
                    log.trace("Evaluating filter against SimpleFeature");
                    if (filter.evaluate(reusable)) {
                        return ReturnCode.INCLUDE;
                    } else {
                        return ReturnCode.SKIP;
                    }
                } else {
                    KryoFeatureSerializer serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId());
                    SimpleFeature sf = serializer.deserialize(v.getValueArray(), v.getValueOffset(), v.getValueLength());
                    log.trace("Evaluating filter against SimpleFeature");
                    if (filter.evaluate(sf)) {
                        return ReturnCode.INCLUDE;
                    } else {
                        return ReturnCode.SKIP;
                    }
                }
            } catch(Exception e) {
                log.error("Exception thrown while scanning, skipping", e);
                return ReturnCode.SKIP;
            }
        }
    }

    public JSimpleFeatureFilter(SimpleFeatureType sft,
                                org.opengis.filter.Filter filter,
                                String transform,
                                String transformSchema) {
        this.sft = sft;
        this.filter = filter;
        this.sftString = SimpleFeatureTypes.encodeType(sft, true);
        this.filterString = ECQL.toCQL(filter);

        this.transformSchema = transformSchema;
        this.transform = transform;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        return ReturnCode.INCLUDE;
    }

    // TODO: Add static method to compute byte array from SFT and Filter.
    @Override
    public byte[] toByteArray() throws IOException {
        return Bytes.add(Bytes.add(getLengthArray(sftString), getLengthArray(filterString), getLengthArray(transform)), getLengthArray(transformSchema));
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

        int transformLen = Bytes.readAsInt(pbBytes, sftLen + filterLen + 8, 4);
        String transformString = new String(Bytes.copy(pbBytes, sftLen + filterLen + 12, transformLen));

        int transformSchemaLen = Bytes.readAsInt(pbBytes, sftLen + filterLen + transformLen + 12, 4);
        String transformSchemaString = new String(Bytes.copy(pbBytes, sftLen + filterLen + transformLen + 16, transformSchemaLen));

        try {
            if("".equals(filterString)) {
                return new IncludeFilter(sftString, filterString, transformString, transformSchemaString);
            } else {
                return new CQLFilter(sftString, filterString, transformString, transformSchemaString);
            }
        } catch (Exception e) {
            throw new DeserializationException(e);
        }
    }
}
