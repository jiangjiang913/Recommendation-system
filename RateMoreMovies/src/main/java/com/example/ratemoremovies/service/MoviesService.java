package com.example.ratemoremovies.service;

import com.example.ratemoremovies.entity.Movies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;

/**
 * Copyright (C), 2018-2022, XXX技有限公司
 *
 * @author: jiangjiang
 * @date: 2022/5/13 9:43
 * @description:
 * @version: 1.0
 * History:
 * <author>   <time>   <version>   <desc>
 * 作者姓名 修改时间  版本号   描述
 */
public class MoviesService {
    @Autowired
    private static MongoTemplate mongoTemplate;
    public static List<Movies> findAll() {
        return mongoTemplate.findAll(Movies.class);
    }

}
