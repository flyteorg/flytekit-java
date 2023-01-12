package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;

import java.io.IOException;
import java.util.Map;

public class IntegerSerializer extends PrimitiveSerializer {


    public IntegerSerializer(JsonGenerator gen, String key, Literal value, SerializerProvider serializerProvider, Map<String, LiteralType> literalTypeMap) {
        super(gen, key, value, serializerProvider, literalTypeMap);
    }

    @Override
    public void serializePrimitive() throws IOException {
        writePrimitive(Primitive.Kind.INTEGER_VALUE,
                (gen, primitive) -> gen.writeNumber(primitive.integerValue()));
    }
}
