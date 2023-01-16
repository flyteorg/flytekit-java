package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Struct;

import java.io.IOException;
import java.util.Map;

public class GenericSerializer extends ScalarSerializer {
    public GenericSerializer(JsonGenerator gen, String key, Literal value, SerializerProvider serializerProvider, LiteralType literalType) {
        super(gen, key, value, serializerProvider, literalType);
        if (literalType.getKind() != LiteralType.Kind.SIMPLE_TYPE && literalType.simpleType() != SimpleType.STRUCT) {
            throw new IllegalArgumentException("Literal type should be a struct literal type");
        }
    }

    @Override
    public void serializeScalar() throws IOException {
        gen.writeObject(Scalar.Kind.GENERIC);
        for (Map.Entry<String, Struct.Value> entry : value.scalar().generic().fields().entrySet()) {
            gen.writeFieldName(entry.getKey());
            serializeStructValue(entry.getValue());
        }
    }

    private void serializeStructValue(
            Struct.Value value)
            throws IOException {
        if (!value.kind().equals(Struct.Value.Kind.LIST_VALUE) && !value.kind().equals(Struct.Value.Kind.NULL_VALUE)) {
            gen.writeStartObject();
            gen.writeFieldName("literal");
            gen.writeObject(Literal.Kind.SCALAR);
            gen.writeFieldName("scalar");
            gen.writeObject(Scalar.Kind.GENERIC);
        }

        if (isSimpleType(value.kind())) {
            gen.writeFieldName("structType");
        }
        switch (value.kind()) {

            case BOOL_VALUE:
                writeSimpleType(Struct.Value.Kind.BOOL_VALUE, value, (generator, v) -> generator.writeBoolean(v.boolValue()));
                return;

            case LIST_VALUE:
                throw new RuntimeException("not supported list inside the struct");

            case NUMBER_VALUE:
                writeSimpleType(Struct.Value.Kind.NUMBER_VALUE, value, (generator, v) -> generator.writeNumber(v.numberValue()));
                return;

            case STRING_VALUE:
                writeSimpleType(Struct.Value.Kind.STRING_VALUE, value, (generator, v) -> generator.writeString(v.stringValue()));
                return;

            case STRUCT_VALUE:
                value.structValue().fields().forEach((k, v) -> writeStructValue(gen, k, v));
                gen.writeEndObject();
                return;

            case NULL_VALUE:
                gen.writeNull();
        }
    }

    private void writeStructValue(JsonGenerator gen, String k, Struct.Value v) {
        try {
            gen.writeFieldName(k);
            serializeStructValue(v);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isSimpleType(Struct.Value.Kind kind) {
        return kind.equals(Struct.Value.Kind.BOOL_VALUE)
                || kind.equals(Struct.Value.Kind.NUMBER_VALUE)
                || kind.equals(Struct.Value.Kind.STRING_VALUE);
    }

    private void writeSimpleType(Struct.Value.Kind kind, Struct.Value structValue, WriteGenericFunction writeTypeFunction) throws IOException {
        gen.writeObject(kind);
        gen.writeFieldName("structValue");
        writeTypeFunction.write(gen, structValue);
        gen.writeEndObject();
    }

    interface WriteGenericFunction {
        void write(JsonGenerator gen, Struct.Value value) throws IOException;
    }
}
