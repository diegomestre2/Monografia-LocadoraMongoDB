/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.util;

import java.sql.Connection;
import java.sql.DriverManager;
import org.kohsuke.rngom.digested.Main;

/**
 *
 * @author Washington
 */
public class ConexaoUtil {

    public static Connection getConnection() throws Exception {
        Connection connection = null;
        String url = "jdbc:postgresql://localhost:5432/Locadora";
        String login = "postgres";
        String senha = "postgres";
        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(url, login, senha);

        return connection;

    }


}
  