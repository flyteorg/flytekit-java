package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

import java.io.IOException;
import java.util.Map;

public abstract class ScalarSerializer extends LiteralSerializer {

    public ScalarSerializer(JsonGenerator gen, String key, Literal value, SerializerProvider serializerProvider, LiteralType literalType) {
        super(gen, key, value, serializerProvider, literalType);
    }

    @Override
    final void serializeLiteral() throws IOException {
        gen.writeObject(Literal.Kind.SCALAR);
        gen.writeFieldName("scalar");
        serializeScalar();
    }

    abstract void serializeScalar() throws IOException;
}
