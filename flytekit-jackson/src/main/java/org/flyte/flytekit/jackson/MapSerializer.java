package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

import java.io.IOException;

public class MapSerializer extends LiteralSerializer {
    public MapSerializer(JsonGenerator gen, String key, Literal value, SerializerProvider serializerProvider, LiteralType literalType) {
        super(gen, key, value, serializerProvider, literalType);
        if (literalType.getKind() != LiteralType.Kind.MAP_VALUE_TYPE) {
            throw new IllegalArgumentException("Literal type should be a Map literal type");
        }
    }

    @Override
    void serializeLiteral() throws IOException {
        gen.writeObject(Literal.Kind.MAP);
        LiteralType valueType = literalType.mapValueType();
        LiteralTypeSerializer.serialize(valueType, gen);
        gen.writeFieldName("value");
        gen.writeStartObject();

        value.map().forEach(this::writeMapEntry);
        gen.writeEndObject();

    }

    private void writeMapEntry(String k, Literal v) {
        try {
            gen.writeFieldName(k);
            gen.writeStartObject();
            LiteralSerializer literalSerializer = LiteralSerializerFactory.create(k, v, gen, serializerProvider, literalType);
            literalSerializer.serialize();
            gen.writeEndObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
