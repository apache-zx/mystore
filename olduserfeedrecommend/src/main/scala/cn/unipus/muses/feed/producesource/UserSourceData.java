package cn.unipus.muses.feed.producesource;

import java.io.*;

/**
 * id bigint,主键id
 * nickname varchar,昵称
 * avatar varchar,头像地址
 * gender smallint,性别
 * birthday datetime,生日
 * phase smallint,学段
 * level smallint,年级
 * begin_year int,入学年份
 * register_source,注册来源
 * province_id bigint,所在地区省id
 * province_name varchar,所在地区省名称
 * city_id bigint,所在地区市id
 * city_name varchar,所在地区市名字
 * create_time datetime,创建时间
 * update_time datetime,修改时间
 * del_flag bit，删除标记
 *
 * 2.创建临时表dwd_user_look_label_tmp
 * create external table if not exists dwd_user_look_label_tmp(
 * user_id string,
 * label string,
 * label_count bigint
 * ) COMMENT '用户标签临时表'
 * partitioned by(dt string)
 * row format delimited fields terminated by '\t'
 * stored as textfile;
 *
 */
public class UserSourceData {

    public static void main(String[] args) {
        try {
            //File file = new File("/opt/module/user.txt");
            File file = new File("d://soft//user.txt");
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            for (int i = 10000; i < 20000; i++) {
                bufferedWriter.write(i + "\t" + "昵称\t头像地址\t男\t1970-01-01\t学段"+(int)(Math.random()*6 +1)+"\t"+(int)(Math.random()*9+100)+
                        "\t2020-11-11\t上海\t11\t上海\t12\t上海\t1999-09-09\t2021-11-11\t0\n");
            }
            bufferedWriter.close();
            System.out.println("success");
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
