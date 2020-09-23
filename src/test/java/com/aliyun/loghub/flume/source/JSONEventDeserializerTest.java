package com.aliyun.loghub.flume.source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import org.apache.flume.Context;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

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

    @Test
    public void testAddTopic(){
        Context context = new Context();
        //context.put("topicAsField","true");
        JSONEventDeserializer jsonEventDeserializer = new JSONEventDeserializer();
        jsonEventDeserializer.configure(context);
        System.out.println(new Gson().toJson(jsonEventDeserializer));
    }

    @Test
    public void testTimeParser() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-M-dd HH:mm:ss");
        LocalDateTime time = LocalDateTime.parse("2020-9-23 10:45:00", formatter);
        System.out.println((int) (time.toEpochSecond(ZoneOffset.UTC)));
    }
}
