package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.SkuOrder;
import com.atguigu.gmallpublisher.service.SugarService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class SugarServiceImpl implements SugarService {

    @Autowired
    private SkuOrder skuOrder;

    @Override
    public BigDecimal getGmv(int date) {
        return skuOrder.selectGmv(date);
    }
}
