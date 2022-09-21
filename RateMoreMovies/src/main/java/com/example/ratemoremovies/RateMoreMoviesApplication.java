package com.example.ratemoremovies;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude= DataSourceAutoConfiguration.class)
public class RateMoreMoviesApplication {

    public static void main(String[] args) {
        SpringApplication.run(RateMoreMoviesApplication.class, args);
    }

}
