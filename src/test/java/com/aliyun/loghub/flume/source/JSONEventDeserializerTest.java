package com.aliyun.loghub.flume.source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class JSONEventDeserializerTest {


    @Test
    public void testAutoDetectJSON() {
        String validJSONArray = "[{\"foor\":\"bar\"}]";
        assertTrue(JSONEventDeserializer.mayBeJSON(validJSONArray));
        Object object = JSONEventDeserializer.parseJSONObjectOrArray(validJSONArray);
        assertTrue(object instanceof JSONArray);

        String validJSONObject = "{\"foo\":\"bar\"}";
        assertTrue(JSONEventDeserializer.mayBeJSON(validJSONObject));
        Object result = JSONEventDeserializer.parseJSONObjectOrArray(validJSONObject);
        assertTrue(result instanceof JSONObject);
    }
}
