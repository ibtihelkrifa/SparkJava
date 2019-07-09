package com.example.demo.FileHandler.controllers;


import com.example.demo.FileHandler.services.TestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;
import scala.tools.nsc.typechecker.Macros;
import scala.util.Success;

@RestController
public class TestController {


    @Autowired
    TestService testService;



    @PostMapping("/uploadFile")
    public ResponseEntity<String>  uploadFile(@RequestParam("file") MultipartFile file) {

        if(file.isEmpty())
        {
            return new ResponseEntity<>("EMpty File ", HttpStatus.BAD_REQUEST);
        }

        else
        {
            int hashcode=file.getName().getBytes().hashCode();
            System.out.println(String.valueOf(hashcode));

            Path  pathInput = Paths.get( "/Files/"+hashcode +".csv");
            try {
                Files.write(pathInput, file.getBytes());
            }

            catch (IOException e) {
                e.printStackTrace();
            }


            return new ResponseEntity<>("File uploade " , HttpStatus.OK);
        }


    }

    @GetMapping("/verifyFile/{idhash}")
    public String getEmptyCells(@PathVariable int idhash)
    {
        return testService.getEmptyCells(idhash);
    }



}
