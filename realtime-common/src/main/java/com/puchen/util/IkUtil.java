package com.puchen.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class IkUtil {

    /**
     * 分词
     * @param ketwords
     * @return
     */
    public static List<String> IKSplit(String ketwords){
        StringReader stringBuilder = new StringReader(ketwords);
        IKSegmenter ikSegmenter = new IKSegmenter(stringBuilder, true);
        ArrayList<String> result = new ArrayList<>();

        try{
            Lexeme next = ikSegmenter.next();
            while (next!=null){
                result.add(next.getLexemeText());
                next = ikSegmenter.next();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return result;
    }


    public static void main(String[] args) throws IOException {
        String s = "Apple/苹果 iPhone 15 Pro Max (A3108) 256GB 原色钛金属 支持移动联通电信5G 双卡双待手机";
        StringReader stringBuilder = new StringReader(s);
        IKSegmenter ikSegmenter = new IKSegmenter(stringBuilder, true);
        Lexeme next = ikSegmenter.next();
        while (next!=null){
            System.out.println(next.getLexemeText());
            next = ikSegmenter.next();
        }
    }
}
