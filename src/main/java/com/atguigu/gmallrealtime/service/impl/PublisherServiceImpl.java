package com.atguigu.gmallrealtime.service.impl;

import com.atguigu.gmallrealtime.mapper.PublisherMapper;
import com.atguigu.gmallrealtime.pojo.NameValue;
import com.atguigu.gmallrealtime.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    PublisherMapper publisherMapper;

    @Override
    public Map<String, Object> detailByItem(String date, String itemName, Integer pageNo, Integer pageSize) {
        Integer from = (pageNo - 1) * pageSize;
        return publisherMapper.detailByItem(date, itemName, from, pageSize);
    }

    @Override
    public List<NameValue> statsByItem(String itemName, String date, String type) {

        // 转换格式
        if("gender".equals(type)) {
            type = "user_gender";
        } else if("age".equals(type)) {
            type = "user_age";
        }
        List<NameValue> nameValueList = publisherMapper.statsByItem(itemName, date, type);
        // 格式化查询得到的数据： F -> 男 M -> 女
        return formatListName(nameValueList, type);
    }

    @Override
    public Map<String, Object> dauRealtime(String dt) {
        return publisherMapper.dauRealtime(dt);
    }

    public List<NameValue> formatListName(List<NameValue> nameValueList, String type) {

        if(nameValueList != null && nameValueList.size() > 0) {

            if("user_gender".equals(type)) {
                // 性别与销售额
                for (NameValue nameValue : nameValueList) {
                    if("F".equals(nameValue.getName())) {
                        nameValue.setName("女");
                    } else if("M".equals(nameValue.getName())) {
                        nameValue.setName("男");
                    }
                }

                return nameValueList;
            }

            if("user_age".equals(type)) {
                // 年龄与销售额
                double totalAmount20 = 0;
                double totalAmount20To29 = 0;
                double totalAmount30 = 0;

                for (NameValue nameValue : nameValueList) {
                    double age = Double.parseDouble(nameValue.getName());
                    double value = Double.parseDouble(nameValue.getValue().toString());

                    if(age < 20) {
                        totalAmount20 += value;
                    } else if(age <= 29) {
                        totalAmount20To29 += value;
                    } else {
                        totalAmount30 += value;
                    }
                }

                // 清空集合
                nameValueList.clear();
                // 赋值
                nameValueList.add(new NameValue("20岁以下", totalAmount20));
                nameValueList.add(new NameValue("20岁至29岁", totalAmount20To29));
                nameValueList.add(new NameValue("30岁以上", totalAmount30));

                return nameValueList;
            }
        }

        return nameValueList;
    }
}
