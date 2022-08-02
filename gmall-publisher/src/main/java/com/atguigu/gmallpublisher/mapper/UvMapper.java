package com.atguigu.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

public interface UvMapper {

    @Select("select ch,sum(uv_ct) uv from dws_traffic_vc_ch_ar_is_new_page_view_window where toYYYYMMDD(stt)=#{date} group by ch order by uv desc limit #{limit}")
    List<Map> selectUvByCh(@Param("date") int date, @Param("limit") int limit);

}
