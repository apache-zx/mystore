package cn.unipus.muses.feed.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class DwdUserLook implements Serializable {
    private String user_id;
    private String goods_list;

    public DwdUserLook() {
    }

    public DwdUserLook(String user_id, String goods_list) {
        this.user_id = user_id;
        this.goods_list = goods_list;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getGoods_list() {
        return goods_list;
    }

    public void setGoods_list(String goods_list) {
        this.goods_list = goods_list;
    }

    @Override
    public String toString() {
        return "DwdUserLook{" +
                "user_id='" + user_id + '\'' +
                ", goods_list='" + goods_list + '\'' +
                '}';
    }
}
