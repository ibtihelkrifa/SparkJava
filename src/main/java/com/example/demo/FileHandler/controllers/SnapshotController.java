package com.example.demo.FileHandler.controllers;


import com.example.demo.FileHandler.services.SnapshotService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SnapshotController {

    @Autowired
    SnapshotService snapshotService;

@GetMapping("/union")
    public void union ()
    {
        this.snapshotService.unionPolicyFiles();
    }


}
