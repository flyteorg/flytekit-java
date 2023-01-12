package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.api.v1.Blob;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Scalar;

import java.io.IOException;
import java.util.Map;

public class BlobSerializer extends ScalarSerializer {
    public BlobSerializer(JsonGenerator gen, String key, Literal value, SerializerProvider serializerProvider, Map<String, LiteralType> literalTypeMap) {
        super(gen, key, value, serializerProvider, literalTypeMap);
    }

    @Override
    void serializeScalar() throws IOException {
        gen.writeFieldName("scalar");
        gen.writeObject(Scalar.Kind.BLOB);
        serializerProvider.findValueSerializer(Blob.class).serialize(value.scalar().blob(), gen, serializerProvider);
    }
}
