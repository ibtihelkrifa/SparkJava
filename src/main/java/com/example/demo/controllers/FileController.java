package com.example.demo.controllers;


import com.example.demo.services.FileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FileController {


    @Autowired
    FileService fileService;


    @GetMapping("/getcount/")
    public Integer readFile()
    {
       return this.fileService.getNumberWords();
    }




}
