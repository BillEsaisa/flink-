package com.atguigu.app;

import java.util.Timer;
import java.util.TimerTask;

public class Test01 {

    public static void main(String[] args) {

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {

            }
        }, 0, 5000L);

    }

}
