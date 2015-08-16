/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.util;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 *
 * @author Raimundinha
 */
public class MongoConnection {
       
    private static final String HOST = "localhost";
    private static final int PORT = 27017;
    private static final String DB_NAME = "Locadora";

    private static MongoConnection uniqInstance;
    private static int mongoInstance = 1;

    private MongoClient mongoClient;
    private MongoDatabase db;

    private MongoConnection() {
        //construtor privado
    }

    //garante sempre uma unica instancia
    public static synchronized MongoConnection getInstance() {
        if (uniqInstance == null) {
            uniqInstance = new MongoConnection();
        }
        return uniqInstance;
    }

    //garante um unico objeto mongo
    public MongoDatabase getDB() {
        if (mongoClient == null) {
            mongoClient = new MongoClient(HOST, PORT);
            db = mongoClient.getDatabase(DB_NAME);
            System.out.println("Mongo instance equals :> " + mongoInstance++);
        }
        return db;
    }
}
