package com.aliyun.loghub.flume.source;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

public class LoghubSourceTest {


    @Test
    public void testCreateConsumerGroupName() {
        String consumerGroup = LoghubSource.createConsumerGroupName();
        System.out.println(consumerGroup);
        Pattern pattern = Pattern.compile("[0-9a-z-_]{2,64}");
        Assert.assertTrue(pattern.matcher(consumerGroup).matches());
    }
}
