package com.atguigu.gmallrealtime.controller;

import com.atguigu.gmallrealtime.pojo.NameValue;
import com.atguigu.gmallrealtime.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    /**
     * http://bigdata.gmall.com/detailByItem?date=2021-02-02&itemName=小米手机&pageNo=1&pageSize=20
     */
    @RequestMapping("detailByItem")
    public Map<String, Object> detailByItem(
            @RequestParam("date") String date,
            @RequestParam("itemName") String itemName,
            // required: 是否必须传入
            // defaultValue: 没有传参时的默认值
            @RequestParam(value = "pageNo", required = false, defaultValue = "1") Integer pageNo,
            @RequestParam(value = "pageSize", required = false, defaultValue = "20") Integer pageSize) {
        return publisherService.detailByItem(date, itemName, pageNo, pageSize);
    }

    /**
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=gender
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=age
     */
    @RequestMapping("statsByItem")
    public List<NameValue> statsByItem(
            @RequestParam("itemName") String itemName,
            @RequestParam("date") String date,
            @RequestParam("t") String t) {
        return publisherService.statsByItem(itemName, date, t);
    }

    /**
     * http://bigdata.gmall.com/dauRealtime?td=2022-01-01
     */
    @RequestMapping("dauRealtime")
    public Map<String, Object> dauRealtime(@RequestParam("td") String td) {
        return publisherService.dauRealtime(td);
    }
}
