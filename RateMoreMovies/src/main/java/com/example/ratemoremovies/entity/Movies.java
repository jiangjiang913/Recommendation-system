package com.example.ratemoremovies.entity;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * Copyright (C), 2018-2022, XXX技有限公司
 *
 * @author: jiangjiang
 * @date: 2022/5/13 9:29
 * @description: 创建实体类
 * @version: 1.0
 * History:
 * <author>   <time>   <version>   <desc>
 * 作者姓名 修改时间  版本号   描述
 */
@Data
@Document("RateMoreMovies")//表名
public class Movies {
    private static final long serialVersionUID = 1L;
    @Field
    private ObjectId id;
    @Field("mid")
    private Integer mid;
    @Field("count")//指定字段,mongodb字段与实体类字段相同的话,可以忽略
    private Integer count;
}
