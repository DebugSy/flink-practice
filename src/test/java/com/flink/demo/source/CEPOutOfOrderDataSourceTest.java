package com.flink.demo.source;

import com.flink.demo.cases.common.datasource.CEPOutOfOrderDataSource;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Created by P0007 on 2019/11/27.
 */
public class CEPOutOfOrderDataSourceTest {

    @Test
    public void testReadFile() {
        CEPOutOfOrderDataSource source = new CEPOutOfOrderDataSource("src/main/resources/data/outoforder/CepOutOfOrderData.csv");
        List<List<String>> clicks = source.getClicks();
        Assert.assertTrue(clicks.size() > 0);
    }

}
