package com.flink.demo.cases.case06;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by P0007 on 2019/10/24.
 */
public class AddressUDTF extends TableFunction<String> {

    private Map<String, String> cacheAddre = new HashMap<>();

    @Override
    public void open(FunctionContext context) throws Exception {
        cacheAddre.put("用户A", "北京市朝阳区望京东湖街道3号北京市朝阳区望京东湖街道4号");
        cacheAddre.put("用户B", "北京市朝阳区望京东湖街道3号北京市朝阳区望京东湖街道4号");
        cacheAddre.put("用户C", "北京市朝阳区望京东湖街道2号北京市朝阳区望京东湖街道4号");
        cacheAddre.put("用户D", "北京市朝阳区望京东湖街道3号北京市朝阳区望京东湖街道4号");
    }

    public void eval(String username) {
        if (cacheAddre.containsKey(username)) {
            String addr = cacheAddre.get(username);
            collect(addr);
        }
    }

    @Override
    public TypeInformation<String> getResultType() {
        return TypeInformation.of(String.class);
    }
}
