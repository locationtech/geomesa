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
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.identity.FeatureId;
import org.apache.avro.util.Utf8;

import java.io.*;
import java.util.*;

public class AvroSimpleFeatureTest {


    protected AvroSimpleFeature create(final String schema, final int size, final String id) throws SchemaException {
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

    public List<AvroSimpleFeature> readAvroWithFSRDecoder(final File avroFile, final Schema schema, final SimpleFeatureType sft, final List<String> desiredFeatures) throws IOException {
        final FileInputStream fis = new FileInputStream(avroFile);
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(fis, null);
        final FeatureSpecificReader fsr = new FeatureSpecificReader(desiredFeatures, schema);

        final List<AvroSimpleFeature> sfList = new ArrayList<>();

        List<String> reuse = new LinkedList<>();
        while (!decoder.isEnd() && (reuse = fsr.read(reuse, decoder)) != null) {
            final FeatureId id = new FeatureIdImpl(reuse.get(0));
            final AvroSimpleFeature tmp = new AvroSimpleFeature(id, sft);
            for (int i = 1; i < reuse.size(); i++) {
                tmp.setAttribute(i - 1, reuse.get(i));
            }
            sfList.add(tmp);
        }
        fis.close();

        return sfList;
    }

    public List<AvroSimpleFeature> readAvroWithGenericDecoder(final File avroFile, final Schema schema, final SimpleFeatureType sft) throws IOException {
        final FileInputStream fis = new FileInputStream(avroFile);
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(fis, null);
        final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        final GenericRecord gr = new GenericData.Record(schema);

        final List<AvroSimpleFeature> sfList = new ArrayList<>();
        GenericRecord reuse;
        while (!decoder.isEnd() && (reuse = datumReader.read(gr, decoder)) != null) {
            // must create object to get good timing metrics
            final FeatureId id = new FeatureIdImpl((reuse.get(AvroSimpleFeature.FEATURE_ID_FIELD_NAME)).toString());
            final AvroSimpleFeature tmp = new AvroSimpleFeature(id, sft);
            final List<Schema.Field> fields = new LinkedList<>(schema.getFields());
            fields.remove(schema.getField(AvroSimpleFeature.FEATURE_ID_FIELD_NAME));
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

        AvroSimpleFeature sample = null;

        // Create an avro file with a custom schema but no header...
        final List<AvroSimpleFeature> sfList = new ArrayList<>();
        for (int x = 0; x < numRecords; x++) {
            sfList.add(create(geoSchema, numFields, Integer.toString(x)));
        }

        sample = sfList.get(0);

        // write out avro file
        final File avroFile = writeAvroFile(sfList);

        // Write out old style pipe-delimited file
        final File pipeFile = writePipeFile(sfList);

        // read back in the avro
        final List<AvroSimpleFeature> fsrList = readAvroWithFSRDecoder(avroFile, sample.schema, sample.getFeatureType(), null);

        // read with generic decoder
        final List<AvroSimpleFeature> genericList = readAvroWithGenericDecoder(avroFile, sample.schema, sample.getFeatureType());

        // read back in the pipe file
        final List<SimpleFeature> pipeList = readPipeFile(pipeFile, sample.getFeatureType());

        //Compare the two...
        Assert.assertEquals(sfList.size(), fsrList.size());

        for (int x = 0; x < sfList.size(); x++) {
            AvroSimpleFeature a = sfList.get(0);
            AvroSimpleFeature b = fsrList.get(0);
            AvroSimpleFeature d = genericList.get(0);
            SimpleFeature c = pipeList.get(0);


            Assert.assertEquals(c.getID(), a.getID());
            Assert.assertEquals(c.getID(), b.getID());
            Assert.assertEquals(c.getID(), d.getID());


            Assert.assertEquals(c.getAttributeCount(), a.getAttributeCount());
            Assert.assertEquals(c.getAttributeCount(), b.getAttributeCount());
            Assert.assertEquals(c.getAttributeCount(), d.getAttributeCount());

            for (int i = 0; i < c.getAttributeCount(); i++) {
                Assert.assertEquals(c.getAttribute(i), a.getAttribute(i));
                Assert.assertEquals(c.getAttribute(i), b.getAttribute(i));
                Assert.assertEquals(c.getAttribute(i), d.getAttribute(i));
            }

            Assert.assertEquals(a, b);
            Assert.assertEquals(a, d);
        }
    }

    @Test
    @Ignore
    public void compareTime() throws SchemaException, IOException {

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
            AvroSimpleFeature sf = create(geoSchema, numFields, Integer.toString(x));
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
            final AvroSimpleFeature tmp = new AvroSimpleFeature(new FeatureIdImpl((t.get(AvroSimpleFeature.FEATURE_ID_FIELD_NAME)).toString()), sample.getFeatureType());
            final List<Schema.Field> fields = new LinkedList<>(sample.schema.getFields());
            fields.remove(sample.schema.getField(AvroSimpleFeature.FEATURE_ID_FIELD_NAME));
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

        final List<String> featureToRead = Arrays.asList("f0", "f1", "f3", "f30", "f59");
        System.out.println("Reading features: " + featureToRead);
        final FeatureSpecificReader fsr = new FeatureSpecificReader(featureToRead, sample.schema);
        List<String> reuse = new LinkedList<>();
        while (!decoder.isEnd() && (reuse = fsr.read(reuse, decoder)) != null) {
            // must create SimpleFeature objects to get fair timing metrics
            final FeatureId id = new FeatureIdImpl(reuse.get(0));
            final AvroSimpleFeature tmp = new AvroSimpleFeature(id, sample.getFeatureType());
            for (int i = 1; i < reuse.size(); i++) {
                tmp.setAttribute(i - 1, reuse.get(i));
            }
        }
        fis.close();
        stop = System.currentTimeMillis();
        System.out.println("Time for Feature Skipping Reader: " + Long.toString(stop - start) + "ms");
    }

}
