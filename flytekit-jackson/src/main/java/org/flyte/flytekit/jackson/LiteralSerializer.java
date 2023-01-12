package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

import java.io.IOException;
import java.util.Map;

abstract class LiteralSerializer {

    protected final JsonGenerator gen;
    protected final String key;
    protected final Literal value;
    protected final SerializerProvider serializerProvider;
    protected final Map<String, LiteralType> literalTypeMap;

    public LiteralSerializer(JsonGenerator gen, String key, Literal value, SerializerProvider serializerProvider, Map<String, LiteralType> literalTypeMap) {
        this.gen = gen;
        this.key = key;
        this.value = value;
        this.serializerProvider = serializerProvider;
        this.literalTypeMap = literalTypeMap;
    }

    final void serialize() throws IOException {
        gen.writeFieldName("literal");
        serializeLiteral();
    }

    abstract void serializeLiteral() throws IOException;
}
