package com.example.ratemoremovies.controller;

import com.example.ratemoremovies.entity.Movies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Copyright (C), 2018-2022, XXX技有限公司
 *
 * @author: jiangjiang
 * @date: 2022/5/13 9:32
 * @description: 控制台
 * @version: 1.0
 * History:
 * <author>   <time>   <version>   <desc>
 * 作者姓名 修改时间  版本号   描述
 */
@RestController
@RequestMapping("RateMoreMovies")
public class MoviesController {
    //查询方法
    @Autowired
    private MongoTemplate mongoTemplate;
//    @GetMapping("/list")
//    public List<Movies> get(){
//        return mongoTemplate.findAll(Movies.class);
//    }
    @RequestMapping("/list")
    public List<Movies> list(){
        return mongoTemplate.findAll(Movies.class);
    }
}
