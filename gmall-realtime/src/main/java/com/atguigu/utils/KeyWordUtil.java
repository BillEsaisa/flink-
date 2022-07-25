package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeyWordUtil {

    public static List<String> splitKeyWord(String keyWord) throws IOException {

        //创建集合用于存放切分后的单词
        ArrayList<String> list = new ArrayList<>();

        //创建IK分词对象
        StringReader reader = new StringReader(keyWord);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        //提取分词结果
        Lexeme next = ikSegmenter.next();
        while (next != null) {
            list.add(next.getLexemeText());
            next = ikSegmenter.next();
        }

        //返回集合
        return list;
    }

}
