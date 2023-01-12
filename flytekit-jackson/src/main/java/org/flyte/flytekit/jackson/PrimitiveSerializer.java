package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;

import java.io.IOException;
import java.util.Map;

public abstract class PrimitiveSerializer extends ScalarSerializer {

    public PrimitiveSerializer(JsonGenerator gen, String key, Literal value, SerializerProvider serializerProvider, Map<String, LiteralType> literalTypeMap) {
        super(gen, key, value, serializerProvider, literalTypeMap);
    }

    @Override
    public final void serializeScalar() throws IOException {
        gen.writeObject(Scalar.Kind.PRIMITIVE);
        serializePrimitive();
    }

    abstract void serializePrimitive() throws IOException;


    protected void writePrimitive(Object kind, WritePrimitiveFunction writeValueFunction) throws IOException {
        gen.writeFieldName("primitive");
        gen.writeObject(kind);
        gen.writeFieldName("value");
        writeValueFunction.write(gen, value.scalar().primitive());
    }

    interface WritePrimitiveFunction {
        void write(JsonGenerator gen, Primitive value) throws IOException;
    }
}
