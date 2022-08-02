package com.atguigu.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

public interface SkuOrder {

    @Select("select sum(order_amount) from dws_trade_sku_order_window where toYYYYMMDD(stt)=#{date}")
    BigDecimal selectGmv(int date);

}
