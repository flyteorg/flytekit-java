package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

import java.io.IOException;
import java.util.Map;

public class MapSerializer extends LiteralSerializer {
    public MapSerializer(JsonGenerator gen, String key, Literal value, SerializerProvider serializerProvider, Map<String, LiteralType> literalTypeMap) {
        super(gen, key, value, serializerProvider, literalTypeMap);
    }

    @Override
    void serializeLiteral() throws IOException {
        gen.writeFieldName("literal");
        gen.writeObject(Literal.Kind.MAP);
        gen.writeFieldName("type");
        gen.writeObject(literalTypeMap.get(key).mapValueType().simpleType());
        gen.writeFieldName("value");
        gen.writeStartObject();

        value.map().forEach(this::writeMapEntry);
        gen.writeEndObject();

    }

    private void writeMapEntry(String k, Literal v) {
        try {
            gen.writeFieldName(k);
            gen.writeStartObject();
            LiteralSerializer literalSerializer = LiteralSerializerFactory.create(k, v, gen, serializerProvider, literalTypeMap);
            literalSerializer.serialize();
            gen.writeEndObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
