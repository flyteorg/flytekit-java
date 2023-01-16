package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

import java.io.IOException;

public class CollectionSerializer extends LiteralSerializer {


    public CollectionSerializer(JsonGenerator gen, String key, Literal value, SerializerProvider serializerProvider, LiteralType literalType) {
        super(gen, key, value, serializerProvider, literalType);
        if (literalType.getKind() != LiteralType.Kind.COLLECTION_TYPE) {
            throw new IllegalArgumentException("Literal type should be a Collection literal type");
        }
    }

    @Override
    void serializeLiteral() throws IOException {
        gen.writeObject(Literal.Kind.COLLECTION);
        LiteralType elementType = literalType.collectionType();
        LiteralTypeSerializer.serialize(elementType, gen);

        gen.writeFieldName("value");
        gen.writeStartArray();

        value.collection().forEach(e -> writeCollectionElement(e, elementType));

        gen.writeEndArray();

    }

    private void writeCollectionElement(Literal element, LiteralType elementType) {
        try {
            gen.writeStartObject();
            LiteralSerializer literalSerializer =
                    LiteralSerializerFactory.create(key, element, gen, serializerProvider, elementType);
            literalSerializer.serialize();
            gen.writeEndObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
