package com.atguigu.gmallpublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

//@EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
@MapperScan(basePackages = "com.atguigu.gmallpublisher.mapper")
@SpringBootApplication
public class GmallPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublisherApplication.class, args);
    }

}
