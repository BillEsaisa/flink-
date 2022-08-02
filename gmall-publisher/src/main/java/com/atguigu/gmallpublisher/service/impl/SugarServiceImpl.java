package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.SkuOrder;
import com.atguigu.gmallpublisher.mapper.UvMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SugarServiceImpl implements SugarService {

    @Autowired
    private SkuOrder skuOrder;

    @Autowired
    private UvMapper uvMapper;

    @Override
    public BigDecimal getGmv(int date) {
        return skuOrder.selectGmv(date);
    }

    @Override
    public Map getUvByCh(int date, int limit) {

        //查ClickHouse
        List<Map> mapList = uvMapper.selectUvByCh(date, limit);
        //创建集合
        HashMap<String, BigInteger> result = new HashMap<>();
        //遍历集合
        for (Map map : mapList) {
            result.put((String) map.get("ch"), (BigInteger) map.get("uv"));
        }
        //返回结果
        return result;
    }
}
