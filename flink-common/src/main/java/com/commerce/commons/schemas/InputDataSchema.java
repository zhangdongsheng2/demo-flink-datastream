package com.commerce.commons.schemas;

import com.alibaba.fastjson.JSONObject;
import com.commerce.commons.model.InputData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Metric Schema ，支持序列化和反序列化
 */
public class InputDataSchema implements DeserializationSchema<InputData>, SerializationSchema<InputData> {

    @Override
    public InputData deserialize(byte[] bytes) throws IOException {
        System.out.println("aaaaaaaaaaa");
        return JSONObject.parseObject(new String(bytes), InputData.class);
    }

    @Override
    public boolean isEndOfStream(InputData metricEvent) {
        return false;
    }

    @Override
    public byte[] serialize(InputData metricEvent) {
        return JSONObject.toJSONString(metricEvent).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<InputData> getProducedType() {
        return TypeInformation.of(InputData.class);
    }
}
