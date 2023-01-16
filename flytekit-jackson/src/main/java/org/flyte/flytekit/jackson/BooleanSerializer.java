package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.SimpleType;

import java.io.IOException;
import java.util.Map;

public class BooleanSerializer extends PrimitiveSerializer {
    public BooleanSerializer(JsonGenerator gen, String key, Literal value, SerializerProvider serializerProvider, LiteralType literalType) {
        super(gen, key, value, serializerProvider, literalType);
        if (literalType.getKind() != LiteralType.Kind.SIMPLE_TYPE && literalType.simpleType() != SimpleType.BOOLEAN) {
            throw new IllegalArgumentException("Literal type should be a boolean literal type");
        }
    }

    @Override
    public void serializePrimitive() throws IOException {
        writePrimitive(Primitive.Kind.BOOLEAN_VALUE, (gen, primitive) ->
                gen.writeBoolean(primitive.booleanValue()));
    }
}
