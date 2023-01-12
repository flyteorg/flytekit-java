package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

import java.io.IOException;
import java.util.Map;

public class CollectionSerializer extends LiteralSerializer {


    public CollectionSerializer(JsonGenerator gen, String key, Literal value, SerializerProvider serializerProvider, Map<String, LiteralType> literalTypeMap) {
        super(gen, key, value, serializerProvider, literalTypeMap);
    }

    @Override
    void serializeLiteral() throws IOException {
        gen.writeFieldName("literal");
        gen.writeObject(Literal.Kind.COLLECTION);
        gen.writeFieldName("type");
        gen.writeObject(literalTypeMap.get(key).collectionType().simpleType());
        gen.writeFieldName("value");
        gen.writeStartArray();

        value.collection().forEach(this::writeCollectionElement);

        gen.writeEndArray();

    }

    private void writeCollectionElement(Literal element) {
        try {
            gen.writeStartObject();
            LiteralSerializer literalSerializer =
                    LiteralSerializerFactory.create(key, element, gen, serializerProvider, literalTypeMap);
            literalSerializer.serialize();
            gen.writeEndObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
