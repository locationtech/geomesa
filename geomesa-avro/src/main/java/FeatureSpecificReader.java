import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class FeatureSpecificReader implements DatumReader<List<String>> {

    final List<String> fieldsDesired;
    final int numFields;

    public FeatureSpecificReader(List<String> fieldsDesired, Schema schema) {
        this.fieldsDesired = fieldsDesired;
        this.schema = schema;
        this.numFields = schema.getFields().size();
    }

    //TODO constructor with fieldIndexes

    Schema schema;

    @Override
    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    /**
     * returns ID + values in list (length attrs + 1 for id)
     **/
    public List<String> read(List<String> reuse, final Decoder in) throws IOException {
        if (reuse != null)
            reuse.clear();
        else
            reuse = new LinkedList<>();

        //read the id always
        reuse.add(in.readString());

        if (fieldsDesired != null) {
            final List<Schema.Field> fields = new LinkedList<>(schema.getFields());
            fields.remove(this.schema.getField(AvroSimpleFeature.FEATURE_ID_FIELD_NAME));

            for (final Schema.Field f : fields) {
                //TODO consider types other than string
                if (fieldsDesired.contains(f.name())) {
                    reuse.add(in.readString());
                } else {
                    in.skipString();
                }
            }
            return reuse;
        }
        //TODO read using field IDs
        else {
            // numFields -1 to compensate for ID
            for (int i = 0; i < numFields - 1; i++) {
                reuse.add(in.readString());
            }
        }
        return reuse;
    }

}
