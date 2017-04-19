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
import org.apache.hadoop.hbase.util.Strings;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.features.interop.SerializationOptions;
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature;
import org.locationtech.geomesa.filter.factory.FastFilterFactory;
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

    private String transform;
    private String transformSchema;

    private interface Filter {
        ReturnCode filterKeyValue(Cell v) throws IOException;

        void setReusableSF(KryoBufferSimpleFeature reusableSF);
    }

    private static abstract class AbstractFilter implements JSimpleFeatureFilter.Filter {
        KryoBufferSimpleFeature sf;

        @Override
        public void setReusableSF(KryoBufferSimpleFeature reusableSF) {
            this.sf = reusableSF;
        }
    }

    private static class IncludeFilter extends JSimpleFeatureFilter.AbstractFilter {
        @Override
        public ReturnCode filterKeyValue(Cell v) throws IOException {
            return ReturnCode.INCLUDE;
        }
    }

    private static class CQLFilter extends JSimpleFeatureFilter.AbstractFilter {
        private org.opengis.filter.Filter filter;
        private static Logger log = LoggerFactory.getLogger(CQLFilter.class);

        CQLFilter(org.opengis.filter.Filter filter) {
            this.filter = filter;
        }

        @Override
        public ReturnCode filterKeyValue(Cell v) throws IOException {
            try {
                log.trace("Evaluating filter against SimpleFeature");
                if (filter.evaluate(sf)) {
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

    private interface Transformer {
        Cell transformCell(Cell v) throws IOException;
        void setReusableSF(KryoBufferSimpleFeature reusableSF);
    }

    private static abstract class AbstractTransformer implements Transformer {

        KryoBufferSimpleFeature sf;
        public void setReusableSF(KryoBufferSimpleFeature reusableSF) {
            this.sf = reusableSF;
        }

    }

    private static class NoTransform extends AbstractTransformer {

        @Override
        public Cell transformCell(Cell v) throws IOException {
            return v;
        }
    }

    private static class CQLTransfomer extends AbstractTransformer {
        private final String transform;
        private final SimpleFeatureType schema;

        CQLTransfomer(String transform, SimpleFeatureType schema) {
            this.transform = transform;
            this.schema = schema;
        }

        @Override
        public void setReusableSF(KryoBufferSimpleFeature reusableSF) {
            super.setReusableSF(reusableSF);
            sf.setTransforms(transform, schema);
        }

        @Override
        public Cell transformCell(Cell c) throws IOException {
            // TODO: transforms are not working
            byte[] newval = sf.transform();
            return CellUtil.createCell(c.getRow(), c.getFamily(), c.getQualifier(), c.getTimestamp(), c.getTypeByte(), newval);
//            return new KeyValue(CellUtil.cloneRow(c), CellUtil.cloneFamily(c), CellUtil.cloneQualifier(c), newval);
        }
    }

    public static class LocalFilterTransformer extends FilterBase {
        private final String transform;
        private final String transformSchema;
        //private final Function3<byte[], Object, Object, String> getId;
        private String sftString;
        private String filterString;
        protected SimpleFeatureType sft;
        private JSimpleFeatureFilter.Filter filter;
        private JSimpleFeatureFilter.Transformer transformer;
        private KryoBufferSimpleFeature reusable;

        LocalFilterTransformer(String sftString,
                               String filterString,
                               String transform,
                               String transformSchema) throws CQLException {
            this.sftString = sftString;
            this.filterString = filterString;
            this.transform = transform;
            this.transformSchema = transformSchema;
            this.filter = buildFilter(filterString);
            this.transformer = buildTransformer(transform, transformSchema);

            sft = IteratorCache.sft(sftString);
            reusable = IteratorCache.serializer(sftString, SerializationOptions.withoutId()).getReusableFeature();

            this.filter.setReusableSF(reusable);
            this.transformer.setReusableSF(reusable);

            // TODO: pass index type into filter from client rather than hardcoding HBaseZ3Index
        }

        @Override
        public ReturnCode filterKeyValue(Cell v) throws IOException {
            // TODO: is visibility filter first in the FilterList?
            // TODO: why do we have to clone the value here?
            // NOTE: the reusable sf buffer is set here and the filter and transformer depend on it
            reusable.setBuffer(CellUtil.cloneValue(v));
            // TODO: avoid boxing if possible
//            String id = getId.apply(v.getRowArray(), new Integer(v.getRowOffset()), new Integer(v.getRowLength()));
//            reusable.setId(id);
            return filter.filterKeyValue(v);
        }

        @Override
        public Cell transformCell(Cell v) throws IOException {
            return transformer.transformCell(v);
        }

        public static org.apache.hadoop.hbase.filter.Filter parseFrom(final byte [] pbBytes) throws DeserializationException {
            // Required due to weird reflection
            return JSimpleFeatureFilter.parseFrom(pbBytes);
        }

        @Override
        public byte[] toByteArray() throws IOException {
            return JSimpleFeatureFilter.toByteArray(sftString, filterString, transform, transformSchema);
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
        byte[][] arrays = {getLengthArray(sftString), getLengthArray(filterString), getLengthArray(transform), getLengthArray(transformSchema)};
        return Bytes.add(arrays);
    }

    public static byte[] toByteArray(String sftString, String filterString, String transform, String transformSchema) throws IOException {
        byte[][] arrays = {getLengthArray(sftString), getLengthArray(filterString), getLengthArray(transform), getLengthArray(transformSchema)};
        return Bytes.add(arrays);
    }

    public static byte[] getLengthArray(String s) {
        int len = getLen(s);
        if (len == 0) {
            return Bytes.toBytes(0);
        } else {
            return Bytes.add(Bytes.toBytes(len), s.getBytes());
        }
    }

    public static int getLen(String s) {
        if (s != null) {
            return s.length();
        } else {
            return 0;
        }
    }

    public static org.apache.hadoop.hbase.filter.Filter parseFrom(final byte [] pbBytes) throws DeserializationException {
        int sftLen =  Bytes.readAsInt(pbBytes, 0, 4);
        String sftString = new String(pbBytes, 4, sftLen);

        int filterLen = Bytes.readAsInt(pbBytes, sftLen + 4, 4);
        String filterString = new String(pbBytes, sftLen + 8, filterLen);

        int transformLen = Bytes.readAsInt(pbBytes, sftLen + filterLen + 8, 4);
        String transformString = new String(pbBytes, sftLen + filterLen + 12, transformLen);

        int transformSchemaLen = Bytes.readAsInt(pbBytes, sftLen + filterLen + transformLen + 12, 4);
        String transformSchemaString = new String(pbBytes, sftLen + filterLen + transformLen + 16, transformSchemaLen);


        try {
            return new LocalFilterTransformer(sftString, filterString, transformString, transformSchemaString);
        } catch (Exception e) {
            throw new DeserializationException(e);
        }
    }

    private static JSimpleFeatureFilter.Filter buildFilter(String filterString) throws CQLException {
        if(!"".equals(filterString)) {
            return new CQLFilter(FastFilterFactory.toFilter(filterString));
        } else {
            return new IncludeFilter();
        }
    }

    private static JSimpleFeatureFilter.Transformer buildTransformer(String transformString, String transformSchemaString) {
        if(Strings.isEmpty(transformString)) {
            return new NoTransform();
        } else {
            return new CQLTransfomer(transformString, SimpleFeatureTypes.createType("", transformSchemaString));
        }
    }

}
