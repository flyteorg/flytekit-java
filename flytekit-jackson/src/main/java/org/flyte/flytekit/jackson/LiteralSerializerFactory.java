package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

import java.util.Map;

public class LiteralSerializerFactory {
    public static LiteralSerializer create(String key, Literal value, JsonGenerator gen, SerializerProvider serializerProvider, Map<String, LiteralType> literalTypeMap) {
        switch (value.kind()) {
            case SCALAR:
                return createScalarSerializer(gen, key, value, serializerProvider, literalTypeMap);
            case MAP:
                return new MapSerializer(gen, key, value, serializerProvider, literalTypeMap);
            case COLLECTION:
                return new CollectionSerializer(gen, key, value, serializerProvider, literalTypeMap);
        }
        throw new AssertionError("Unexpected Literal.Kind: [" + value.kind() + "]");
    }

    private static ScalarSerializer createScalarSerializer(JsonGenerator gen, String key, Literal value, SerializerProvider serializerProvider, Map<String, LiteralType> literalTypeMap) {
        switch (value.scalar().kind()) {
            case PRIMITIVE:
                return createPrimitiveSerializer(gen, key, value, serializerProvider, literalTypeMap);
            case GENERIC:
                return new GenericSerializer(gen, key, value, serializerProvider, literalTypeMap);
            case BLOB:
                return new BlobSerializer(gen, key, value, serializerProvider, literalTypeMap);
        }
        throw new AssertionError("Unexpected Literal.Kind: [" + value.scalar().kind() + "]");
    }

    private static PrimitiveSerializer createPrimitiveSerializer(JsonGenerator gen, String key, Literal value, SerializerProvider serializerProvider, Map<String, LiteralType> literalTypeMap) {
        switch (value.scalar().primitive().kind()) {
            case INTEGER_VALUE:
                return new IntegerSerializer(gen, key, value, serializerProvider, literalTypeMap);
            case FLOAT_VALUE:
                return new FloatSerializer(gen, key, value, serializerProvider, literalTypeMap);
            case STRING_VALUE:
                return new StringSerializer(gen, key, value, serializerProvider, literalTypeMap);
            case BOOLEAN_VALUE:
                return new BooleanSerializer(gen, key, value, serializerProvider, literalTypeMap);
            case DATETIME:
                return new DatetimeSerializer(gen, key, value, serializerProvider, literalTypeMap);
            case DURATION:
                return new DurationSerializer(gen, key, value, serializerProvider, literalTypeMap);
        }
        throw new AssertionError("Unexpected Primitive.Kind: [" + value.scalar().primitive().kind() + "]");
    }
}
