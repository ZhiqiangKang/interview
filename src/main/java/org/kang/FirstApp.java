package org.kang;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * 第一题
 *
 * @author Kang
 * @version 1.0
 * @time 2019-07-21 10:02
 */
public class FirstApp implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirstApp.class);

    public static void main(String[] args) {
        new FirstApp().run();
    }

    private void run(){

    }
}