package com.commerce.commons.schemas;

import com.alibaba.fastjson.JSONObject;
import com.commerce.commons.model.InputData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Metric Schema ，支持序列化和反序列化
 */
public class InputDataSchema implements DeserializationSchema<InputData>, SerializationSchema<InputData> {
    private static final Logger LOG = LoggerFactory.getLogger(InputDataSchema.class);

    @Override
    public InputData deserialize(byte[] bytes) {
        try {
            return JSONObject.parseObject(new String(bytes), InputData.class);
        } catch (Exception e) {
            LOG.info("数据转换失败: {}", new String(bytes));
            InputData inputData = new InputData();
            inputData.setInputData(new String(bytes));
            return inputData;
        }
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
