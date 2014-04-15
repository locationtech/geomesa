import com.vividsolutions.jts.geom.*;
import geomesa.utils.geohash.GeohashUtils;
import geomesa.utils.text.WKTUtils;
import geomesa.utils.text.WKTUtils$;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.filter.identity.FeatureIdImpl;
import org.junit.Ignore;
import org.junit.Test;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.identity.FeatureId;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class AvroSimpleFeatureTest {


    protected AvroSimpleFeature createStringFeatures(final String schema, final int size, final String id) throws SchemaException {
        final SimpleFeatureType sft = DataUtilities.createType("test", schema);
        final Random r = new Random();
        r.setSeed(0);

        final List<String> l = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            l.add(randomString(i, 8, r));
        }

        final FeatureId fid = new FeatureIdImpl(id);
        final AvroSimpleFeature simpleFeature = new AvroSimpleFeature(fid, sft);
        for (int i = 0; i < l.size(); i++) {
            simpleFeature.setAttribute(i, l.get(i));
        }
        return simpleFeature;
    }

    protected AvroSimpleFeature createFancyType(final String schema, final String id) throws SchemaException, ParseException {
        final SimpleFeatureType sft = DataUtilities.createType("test", schema);
        final Random r = new Random();
        r.setSeed(0);

        final FeatureId fid = new FeatureIdImpl(id);
        final AvroSimpleFeature simpleFeature = new AvroSimpleFeature(fid, sft);
        for (AttributeDescriptor attributeDescriptor : sft.getAttributeDescriptors()) {
            final String name = attributeDescriptor.getLocalName();
            final Class<?> clazz = attributeDescriptor.getType().getBinding();
            if (clazz == String.class) {
                simpleFeature.setAttribute(name, randomString(sft.indexOf(name), 8, r));
            } else if (clazz == Integer.class) {
                simpleFeature.setAttribute(name, Integer.valueOf(r.nextInt()));
            } else if (clazz == Long.class) {
                simpleFeature.setAttribute(name, Long.valueOf(r.nextLong()));
            } else if (clazz == Double.class) {
                simpleFeature.setAttribute(name, Double.valueOf(r.nextDouble()));
            } else if (clazz == Float.class) {
                simpleFeature.setAttribute(name, Float.valueOf(r.nextFloat()));
            } else if (clazz == Boolean.class) {
                simpleFeature.setAttribute(name, Boolean.valueOf(r.nextBoolean()));
            } else if (clazz == UUID.class) {
                final UUID uuid = UUID.fromString("12345678-1234-1234-1234-123456789023");
                simpleFeature.setAttribute(name, uuid);
            } else if (clazz == Date.class) {
                final Date date = new SimpleDateFormat("yyyyMMdd").parse("20140102");
                simpleFeature.setAttribute(name, date);
            } else if (clazz == Point.class) {
                Point p = (Point) GeohashUtils.wkt2geom("POINT(45.0 49.0)");
                simpleFeature.setAttribute(name, p);
            } else if (clazz == Polygon.class) {
                Polygon p = (Polygon) GeohashUtils.wkt2geom("POLYGON((-80 30,-80 23,-70 30,-70 40,-80 40,-80 30))");
                simpleFeature.setAttribute(name, p);
            } else {
                System.out.println(clazz);
                //todo others like geometry
            }
        }
        return simpleFeature;
    }

    protected String randomString(int fieldId, int length, Random r) {
        //byte[] b = new byte[length];
        // r.nextBytes(b);
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(fieldId);
        }
        return sb.toString();
    }

    public File writeAvroFile(final List<AvroSimpleFeature> sfList) throws IOException {
        // Create an avro file with a custom schema but no header...
        final File f = File.createTempFile("abcd", ".tmp");
        f.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream(f);
        for (final AvroSimpleFeature sf : sfList) {
            sf.write(fos);
        }
        fos.close();
        return f;
    }

    // Create an old-style pipe-delimited text file
    public File writePipeFile(final List<AvroSimpleFeature> sfList) throws IOException {
        final File pipeFile = File.createTempFile("pipe", ".tmp");
        pipeFile.deleteOnExit();
        final BufferedWriter pipeWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(pipeFile)));
        for (final AvroSimpleFeature tmp : sfList) {
            pipeWriter.write(DataUtilities.encodeFeature(tmp, true));
            pipeWriter.newLine();
        }
        pipeWriter.close();
        return pipeFile;
    }

    public List<AvroSimpleFeature> readAvroWithFSRDecoder(final File avroFile, final SimpleFeatureType oldType, final SimpleFeatureType newType) throws IOException {
        System.out.println("Reading subset of features: " + Arrays.asList(DataUtilities.attributeNames(newType)));
        final FileInputStream fis = new FileInputStream(avroFile);
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(fis, null);
        final FeatureSpecificReader fsr = new FeatureSpecificReader(oldType, newType);

        final List<AvroSimpleFeature> sfList = new ArrayList<>();

        AvroSimpleFeature reuse = null;
        while (!decoder.isEnd() && (reuse = fsr.read(null, decoder)) != null) {
            sfList.add(reuse);
        }
        fis.close();

        return sfList;
    }

    public List<AvroSimpleFeature> readAvroWithGenericDecoder(final File avroFile, final SimpleFeatureType sft) throws IOException {
        final FileInputStream fis = new FileInputStream(avroFile);
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(fis, null);
        Schema schema = AvroSimpleFeature.generateSchema(sft);
        final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        final GenericRecord gr = new GenericData.Record(schema);

        final List<AvroSimpleFeature> sfList = new ArrayList<>();
        GenericRecord reuse;
        while (!decoder.isEnd() && (reuse = datumReader.read(gr, decoder)) != null) {
            // must create object to get good timing metrics
            final FeatureId id = new FeatureIdImpl((reuse.get(AvroSimpleFeature.FEATURE_ID_AVRO_FIELD_NAME)).toString());
            final AvroSimpleFeature tmp = new AvroSimpleFeature(id, sft);
            final List<Schema.Field> fields = new LinkedList<>(schema.getFields());
            fields.remove(schema.getField(AvroSimpleFeature.FEATURE_ID_AVRO_FIELD_NAME));
            fields.remove(schema.getField(AvroSimpleFeature.AVRO_SIMPLE_FEATURE_VERSION));
            for (final Schema.Field f : fields) {
                tmp.setAttribute(f.name(), (reuse.get(f.name())).toString());
            }
            sfList.add(tmp);
        }
        fis.close();
        return sfList;
    }

    public List<SimpleFeature> readPipeFile(final File pipeFile, final SimpleFeatureType sft) throws IOException {
        // read back in the pipe file
        final BufferedReader pipeReader = new BufferedReader(new InputStreamReader(new FileInputStream(pipeFile)));
        final List<SimpleFeature> sfList = new ArrayList<>();
        String line;
        while ((line = pipeReader.readLine()) != null) {
            sfList.add(DataUtilities.createFeature(sft, line));
        }
        pipeReader.close();
        return sfList;
    }

    public List<AvroSimpleFeature> getSubsetData() throws IOException, SchemaException {
        final int numFields = 60;
        final int numRecords = 10;
        // Create field names prepended with f
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numFields; i++) {
            if (i != 0) sb.append(",");
            sb.append(String.format("f%d:String", i));
        }
        final String geoSchema = sb.toString();

        // Create an avro file with a custom schema but no header...
        final List<AvroSimpleFeature> sfList = new ArrayList<>();
        for (int x = 0; x < numRecords; x++) {
            sfList.add(createStringFeatures(geoSchema, numFields, Integer.toString(x)));
        }

        final SimpleFeatureType oldType = sfList.get(0).getType();

        // write out avro file
        final File avroFile = writeAvroFile(sfList);

        // read back in the avro
        final SimpleFeatureType subsetType = DataUtilities.createType("subsetType", "f0:String,f1:String,f3:String,f30:String,f59:String");
        final List<AvroSimpleFeature> fsrList = readAvroWithFSRDecoder(avroFile, oldType, subsetType);

        return fsrList;
    }

    @Test
    public void testSubsetReturnsExactlySubset() throws IOException, SchemaException {
        final List<AvroSimpleFeature> fsrList = getSubsetData();

        Assert.assertEquals(10, fsrList.size());

        for (final AvroSimpleFeature sf : fsrList) {
            Assert.assertEquals(5, sf.getAttributeCount());
            final List<Object> attrs = sf.getAttributes();
            Assert.assertEquals(5, attrs.size());
            for (Object o : attrs) {
                Assert.assertTrue(o instanceof String);
                String s = (String) o;
                Assert.assertNotNull(s);
            }

            Assert.assertNotNull(sf.getAttribute("f0"));
            Assert.assertNotNull(sf.getAttribute("f1"));
            Assert.assertNotNull(sf.getAttribute("f3"));
            Assert.assertNotNull(sf.getAttribute("f30"));
            Assert.assertNotNull(sf.getAttribute("f59"));
        }
    }

    @Test(expected = NullPointerException.class)
    public void testSubsetNull() throws IOException, SchemaException {
        final List<AvroSimpleFeature> fsrList = getSubsetData();
        for (final AvroSimpleFeature sf : fsrList) {
            sf.getAttribute("f20");  //should not exist in subset data!
        }
    }

    @Test
    public void testCorrectSerializationAndDeserialization() throws IOException, SchemaException {
        final int numFields = 60;
        final int numRecords = 100;

        // Create field names prepended with f
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numFields; i++) {
            if (i != 0) sb.append(",");
            sb.append(String.format("f%d:String", i));
        }
        final String geoSchema = sb.toString();

        // Create an avro file with a custom schema but no header...
        final List<AvroSimpleFeature> sfList = new ArrayList<>();
        for (int x = 0; x < numRecords; x++) {
            sfList.add(createStringFeatures(geoSchema, numFields, Integer.toString(x)));
        }

        final SimpleFeatureType oldType = sfList.get(0).getType();

        // write out avro file
        final File avroFile = writeAvroFile(sfList);

        // Write out old style pipe-delimited file
        final File pipeFile = writePipeFile(sfList);

        // read back in the avro
        final SimpleFeatureType subsetType = DataUtilities.createType("subsetType", "f0:String,f1:String,f3:String,f30:String,f59:String");
        final List<AvroSimpleFeature> fsrList = readAvroWithFSRDecoder(avroFile, oldType, subsetType);

        // read everything back
        final List<AvroSimpleFeature> genericList = readAvroWithGenericDecoder(avroFile, oldType);

        // read back in the pipe file
        final List<SimpleFeature> pipeList = readPipeFile(pipeFile, oldType);

        //Compare the two...
        Assert.assertEquals(sfList.size(), fsrList.size());

        for (int x = 0; x < sfList.size(); x++) {
            AvroSimpleFeature original = sfList.get(0);
            AvroSimpleFeature subset = fsrList.get(0);
            AvroSimpleFeature fullFeature = genericList.get(0);
            SimpleFeature pipeFeature = pipeList.get(0);

            Assert.assertEquals(original.getID(), subset.getID());
            Assert.assertEquals(original.getID(), fullFeature.getID());
            Assert.assertEquals(original.getID(), pipeFeature.getID());

            Assert.assertEquals(60, original.getAttributeCount());
            Assert.assertEquals(60, fullFeature.getAttributeCount());
            Assert.assertEquals(60, pipeFeature.getAttributeCount());

            Assert.assertEquals(5, subset.getAttributeCount());

            Assert.assertEquals(original.getAttributeCount(), pipeFeature.getAttributeCount());
            Assert.assertTrue(original.getAttributeCount() > subset.getAttributeCount());
            Assert.assertEquals(original.getAttributeCount(), fullFeature.getAttributeCount());

            for (int i = 0; i < original.getAttributeCount(); i++) {
                Assert.assertEquals(original.getAttribute(i), pipeFeature.getAttribute(i));
                Assert.assertEquals(original.getAttribute(i), fullFeature.getAttribute(i));
            }

            Assert.assertFalse(original.equals(subset));
            Assert.assertTrue(original.equals(fullFeature));
        }
    }


    @Test
    public void testFancySerializeAndDeserialize() throws SchemaException, IOException, ParseException {
        final int numRecords = 1;

        // Create fields
        // TODO more things
        final String geoSchema = "f0:String,f1:Integer,f2:Double,f3:Float,f4:Boolean,f5:UUID,f6:Date,f7:Point:srid=4326,f8:Polygon:srid=4326";

        // Create an avro file with a custom schema but no header...
        final List<AvroSimpleFeature> sfList = new ArrayList<>();
        for (int x = 0; x < numRecords; x++) {
            sfList.add(createFancyType(geoSchema, Integer.toString(x)));
        }

        final SimpleFeatureType oldType = sfList.get(0).getType();

        // write out avro file
        final File avroFile = writeAvroFile(sfList);

        // Write out old style pipe-delimited file
        final File pipeFile = writePipeFile(sfList);

        // read back in the avro
        final SimpleFeatureType subsetType = DataUtilities.createType("subsetType", "f0:String,f3:Float,f5:UUID,f6:Date");
        final List<AvroSimpleFeature> fsrList = readAvroWithFSRDecoder(avroFile, oldType, subsetType);

        // read with generic decoder
        final List<AvroSimpleFeature> genericList = readAvroWithFSRDecoder(avroFile, oldType, oldType);

        // read back in the pipe file
        final List<SimpleFeature> pipeList = readPipeFile(pipeFile, oldType);

        //Compare the two...
        Assert.assertEquals(sfList.size(), fsrList.size());

        for (int x = 0; x < sfList.size(); x++) {
            AvroSimpleFeature original = sfList.get(0);
            AvroSimpleFeature subset = fsrList.get(0);
            AvroSimpleFeature fullFeature = genericList.get(0);
            SimpleFeature pipeFeature = pipeList.get(0);

            Assert.assertEquals(original.getID(), subset.getID());
            Assert.assertEquals(original.getID(), fullFeature.getID());
            Assert.assertEquals(original.getID(), pipeFeature.getID());

            Assert.assertEquals(9, original.getAttributeCount());
            Assert.assertEquals(9, fullFeature.getAttributeCount());
            Assert.assertEquals(9, pipeFeature.getAttributeCount());

            Assert.assertEquals(4, subset.getAttributeCount());

            Assert.assertEquals(original.getAttributeCount(), pipeFeature.getAttributeCount());
            Assert.assertTrue(original.getAttributeCount() > subset.getAttributeCount());
            Assert.assertEquals(original.getAttributeCount(), fullFeature.getAttributeCount());

            for (int i = 0; i < original.getAttributeCount(); i++) {
                Assert.assertEquals(original.getAttribute(i), pipeFeature.getAttribute(i));
                Assert.assertEquals(original.getAttribute(i), fullFeature.getAttribute(i));
            }

            Assert.assertFalse(original.equals(subset));
            Assert.assertTrue(original.equals(fullFeature));
        }
    }


    @Test
//    @Ignore
    public void compareTimeWithStrings() throws SchemaException, IOException {
        System.out.println("Beginning performance test...");
        final int numFields = 60;
        final int numRecords = 100_000;   // test lots of time to remove error and get better mean
        System.out.printf("Number of fields: %d\n", numFields);
        System.out.println("Number of Records: " + numRecords);

        // Create field names prepended with f
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numFields; i++) {
            if (i != 0) sb.append(",");
            sb.append(String.format("f%d:String", i));
        }
        final String geoSchema = sb.toString();
        System.out.println("Using geo schema " + geoSchema);

        AvroSimpleFeature sample = null;

        // Create an avro file with a custom schema but no header...
        final File avroFile = File.createTempFile("avro", ".tmp");
        avroFile.deleteOnExit();
        final FileOutputStream avroOs = new FileOutputStream(avroFile);

        // Create a regular serialized file
        final File pipeFile = File.createTempFile("pipe", ".tmp");
        pipeFile.deleteOnExit();
        final BufferedWriter pipeWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(pipeFile)));

        //Write to both
        for (int x = 0; x < numRecords; x++) {
            AvroSimpleFeature sf = createStringFeatures(geoSchema, numFields, Integer.toString(x));
            if (x == 0) sample = sf;
            sf.write(avroOs);
            pipeWriter.write(DataUtilities.encodeFeature(sf, true));
            pipeWriter.newLine();
        }

        // Close both
        avroOs.close();
        pipeWriter.close();

        // Test pipe reader
        long pStart = System.currentTimeMillis();
        final BufferedReader pipeReader = new BufferedReader(new InputStreamReader(new FileInputStream(pipeFile)));
        String line;
        while ((line = pipeReader.readLine()) != null) {
            DataUtilities.createFeature(sample.getFeatureType(), line);
        }
        pipeReader.close();
        long pStop = System.currentTimeMillis();
        System.out.println("Time for pipe text: " + Long.toString(pStop - pStart) + "ms");

        // Test regular decoder
        long start = System.currentTimeMillis();
        FileInputStream fis = new FileInputStream(avroFile);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(fis, null);
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(sample.schema);
        final GenericRecord gr = new GenericData.Record(sample.schema);

        GenericRecord t;
        while (!decoder.isEnd() && (t = datumReader.read(gr, decoder)) != null) {
            // must create SimpleFeature objects to get fair timing metrics
            final AvroSimpleFeature tmp = new AvroSimpleFeature(new FeatureIdImpl((t.get(AvroSimpleFeature.FEATURE_ID_AVRO_FIELD_NAME)).toString()), sample.getFeatureType());
            final List<Schema.Field> fields = new LinkedList<>(sample.schema.getFields());
            fields.remove(sample.schema.getField(AvroSimpleFeature.FEATURE_ID_AVRO_FIELD_NAME));
            fields.remove(sample.schema.getField(AvroSimpleFeature.AVRO_SIMPLE_FEATURE_VERSION));
            for (final Schema.Field f : fields) {
                tmp.setAttribute(f.name(), (t.get(f.name())).toString());
            }
        }
        fis.close();
        long stop = System.currentTimeMillis();
        System.out.println("Time for Generic: " + Long.toString(stop - start) + "ms");

        // Test feature skipping decoder (FSR)
        start = System.currentTimeMillis();
        fis = new FileInputStream(avroFile);
        decoder = DecoderFactory.get().binaryDecoder(fis, null);

        final SimpleFeatureType subsetType = DataUtilities.createType("subsetType", "f0:String,f1:String,f3:String,f30:String,f59:String");
        final FeatureSpecificReader fsr = new FeatureSpecificReader(sample.getFeatureType(), subsetType);
        AvroSimpleFeature reuse = null;
        while (!decoder.isEnd() && (reuse = fsr.read(reuse, decoder)) != null) {
            // nothing
        }
        fis.close();
        stop = System.currentTimeMillis();
        System.out.println("Time for Feature Skipping Reader (subset of data): " + Long.toString(stop - start) + "ms");


        // Test feature skipping decoder (FSR)
        start = System.currentTimeMillis();
        fis = new FileInputStream(avroFile);
        decoder = DecoderFactory.get().binaryDecoder(fis, null);

        final FeatureSpecificReader fsr2 = new FeatureSpecificReader(sample.getFeatureType(), sample.getFeatureType());
        while (!decoder.isEnd() && (reuse = fsr.read(reuse, decoder)) != null) {
            // nothing
        }
        fis.close();
        stop = System.currentTimeMillis();
        System.out.println("Time for Feature Skipping Reader (all data): " + Long.toString(stop - start) + "ms");
    }

}
