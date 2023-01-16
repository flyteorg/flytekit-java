package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import org.flyte.api.v1.LiteralType;

import java.io.IOException;

class LiteralTypeSerializer {
    static void serialize(LiteralType literalType, JsonGenerator gen) throws IOException {
        gen.writeFieldName("type");

        gen.writeStartObject();
        gen.writeFieldName("kind");
        gen.writeObject(literalType.getKind());
        gen.writeFieldName("value");
        switch (literalType.getKind()) {

            case SIMPLE_TYPE:
                //{type: {kind: simple, value: string}}
                gen.writeObject(literalType.simpleType());
                break;
            case COLLECTION_TYPE:
                //{type: {kind: collection, value: {type:{{kind: simple, value: string}}}}}}
                gen.writeStartObject();
                serialize(literalType.collectionType(), gen);
                gen.writeEndObject();
                break;
            case MAP_VALUE_TYPE:
                //{type: {kind: map, value: {type:{{kind: simple, value: string}}}}}}
                gen.writeStartObject();
                serialize(literalType.mapValueType(), gen);
                gen.writeEndObject();
                break;
            case SCHEMA_TYPE:
            case BLOB_TYPE:
                throw new IllegalArgumentException(
                        String.format("Unsupported LiteralType.Kind: [%s]", literalType.getKind()));
        }
        gen.writeEndObject();
    }


}
