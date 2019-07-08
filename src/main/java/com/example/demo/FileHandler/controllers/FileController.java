package com.example.demo.FileHandler.controllers;


import com.example.demo.FileHandler.services.FileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class FileController {


    @Autowired
    FileService fileService;


    @GetMapping("/getcount/")
    public Integer readFile()
    {
       return this.fileService.getNumberWords();
    }


  /*  @GetMapping("/getNumberColumns")
    public List<Integer> numberOfColumnsInEachLine()
    {
        return this.fileService.getNumberColumns();

    }*/



}
