package com.atguigu.gmallrealtime.mapper;

import com.atguigu.gmallrealtime.pojo.NameValue;

import java.util.List;
import java.util.Map;

public interface PublisherMapper {
    Map<String, Object> detailByItem(String date, String itemName, Integer pageNo, Integer pageSize);

    List<NameValue> statsByItem(String itemName, String date, String type);

    Map<String, Object> dauRealtime(String td);
}
