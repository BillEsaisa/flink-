package com.atguigu.gmallpublisher.service;

import java.math.BigDecimal;
import java.util.Map;

public interface SugarService {

    BigDecimal getGmv(int date);

    Map getUvByCh(int date, int limit);

}
