package com.atguigu.gmallpublisher.controller;

import com.atguigu.gmallpublisher.service.impl.SugarService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

//@Controller
@RestController
@RequestMapping("/gmall/realtime")
public class SugarController {

    @Autowired
private SugarService sugarService;


    @RequestMapping("gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {

        if (date == 0) {
            date = getToday();
        }

        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": " + sugarService.getGmv(date) +
                "}";

    }

    @RequestMapping("ch")
    public String getUvByCh(@RequestParam(value = "date", defaultValue = "0") int date,
                            @RequestParam(value = "limit", defaultValue = "5") int limit) {
        if (date == 0) {
            date = getToday();
        }

        Map uvByCh = sugarService.getUvByCh(date, limit);

        Set chs = uvByCh.keySet();
        Collection values = uvByCh.values();

        return "{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": {" +
                "    \"categories\": [\"" +
                StringUtils.join(chs, "\",\"") +
                "\"]," +
                "    \"series\": [" +
                "      {" +
                "        \"name\": \"UV\"," +
                "        \"data\": [" +
                StringUtils.join(values, ",") +
                "]" +
                "      }" +
                "    ]" +
                "  }" +
                "}";
    }

    private int getToday() {
        long ts = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(sdf.format(ts));
    }

}
