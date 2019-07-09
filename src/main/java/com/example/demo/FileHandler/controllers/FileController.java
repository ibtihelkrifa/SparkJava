package com.example.demo.FileHandler.controllers;


import com.example.demo.FileHandler.services.FileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class FileController {


    @Autowired
    FileService fileService;


    @GetMapping("/getcount")
    public Integer readFile()
    {
       return this.fileService.getNumberWords();
    }


    @GetMapping("/getNumberColumns")
    public List<Integer> numberOfColumnsInEachLine()
    {
        return this.fileService.getNumberColumns();

    }

    @GetMapping("/replaceByEMpty/{id}")

    public void  putEmptyColumns(@PathVariable String id)
    {
        System.out.println(id);
        this.fileService.putEmptyColumns2(id);
    }

    /*@GetMapping("/replaceByEMpty2/{id}")

    public void  putEmptyColumns2(String id)
    {
        this.fileService.putEmptyColumns2(id);
    }*/

    @GetMapping("/flatmap/{id}")
    public void  flatMapAndReduceByKeyExample(@PathVariable  String id)
    {
        this.fileService.flatMapAndReduceByKeyExample(id);
    }


    @GetMapping("/filter/{id}")
    public void  filterOnWordsLength(@PathVariable  String id)
    {
        System.out.println(id);
        this.fileService.filterOnWordsLength(id);
    }



    @GetMapping("/filterBySex")
    public void filterBySex()
    {
        this.fileService.FilterBySex();
    }
}
