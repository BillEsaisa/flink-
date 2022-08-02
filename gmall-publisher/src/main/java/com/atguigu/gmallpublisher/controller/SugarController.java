package com.atguigu.gmallpublisher.controller;

import com.atguigu.gmallpublisher.service.SugarService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;

//@Controller
@RestController
public class SugarController {

    @Autowired
    private SugarService sugarService;

    @RequestMapping("test")
    //@ResponseBody
    public String test1() {
        System.out.println("aaaaaaaaaaaaaaaaa");
        return "success";
        //return "index.html";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String nn,
                        @RequestParam(value = "age", defaultValue = "18") int age) {
        System.out.println(nn + ":" + age);
        return "success";
    }

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

    private int getToday() {
        long ts = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(sdf.format(ts));
    }

}
