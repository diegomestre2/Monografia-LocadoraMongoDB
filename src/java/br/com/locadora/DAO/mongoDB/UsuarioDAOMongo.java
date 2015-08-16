/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.DAO.mongoDB;

import br.com.locadora.model.Usuario;
import br.com.locadora.util.ConexaoUtil;
import br.com.locadora.util.MongoConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.bson.BSON;
import org.bson.Document;

/**
 *
 * @author Raimundinha
 */
public class UsuarioDAOMongo {

    public void update(Usuario c) throws Exception {
        Connection connection = ConexaoUtil.getConnection();
        StringBuilder sql = new StringBuilder();
        sql.append(" UPDATE usuario SET  nome = ?, login  = ?, senha = ? ");
        sql.append(" WHERE id = ? ");
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setString(1, c.getNome());
        preparedStatement.setString(2, c.getLogin());
        preparedStatement.setString(3, c.getSenha());
        preparedStatement.setLong(4, c.getId());

        preparedStatement.execute();

        preparedStatement.close();
        connection.close();
    }

    public void inserir(Usuario c) throws Exception {
        if (c.getId() != null) {
            this.update(c);
        } else {
            MongoConnection connection = MongoConnection.getInstance();
            connection.getDB().getCollection("usuario").insertOne(new Document()
                            .append("id", 1)
                            .append("nome", c.getNome())
                            .append("login", c.getLogin())
                            .append("senha", c.getSenha())
                    );
   
        }

    }

    public Usuario recuperar(Long id) throws Exception {

        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT id, nome, login, senha FROM usuario where id = ? ");

        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, id);
        ResultSet resultSet = preparedStatement.executeQuery();
        Usuario usuario = null;
        while (resultSet.next()) {
            String nome = resultSet.getString("nome");
            String login = resultSet.getString("login");
            String senha = resultSet.getString("senha");
            usuario = new Usuario(nome, login, senha);
            usuario.setId(resultSet.getLong("id"));
        }

        resultSet.close();
        preparedStatement.close();
        connection.close();
        return usuario;
    }

    public boolean excluir(Long id) throws Exception {

        StringBuilder sql = new StringBuilder();
        sql.append(" DELETE FROM  usuario WHERE id = ? ");

        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, id);
        try {
            preparedStatement.execute();
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            preparedStatement.close();
            connection.close();
        }
    }

    public Usuario buscarUsuario(String login, String senha) throws Exception {

        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT id, nome, login, senha FROM usuario where login = ?  and senha = ? ");
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setString(1, login);
        preparedStatement.setString(2, senha);
        ResultSet resultSet = preparedStatement.executeQuery();
        Usuario usuario = null;
        while (resultSet.next()) {
            String nome = resultSet.getString("nome");
            usuario = new Usuario(nome, login, senha);
            usuario.setId(resultSet.getLong("id"));
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();
        System.out.println(usuario.getLogin() + " " + login);
        return usuario;

    }

    public Usuario buscarUsuarioPorLogin(String login) throws Exception {

        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT id, nome, login, senha FROM usuario where login = ? ");

        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setString(1, login);

        ResultSet resultSet = preparedStatement.executeQuery();
        Usuario usuario = null;
        while (resultSet.next()) {
            String nome = resultSet.getString("nome");
            String senha = resultSet.getString("senha");
            usuario = new Usuario(nome, login, senha);
            usuario.setId(resultSet.getLong("id"));
        }

        resultSet.close();
        preparedStatement.close();
        connection.close();

        return usuario;
    }

    public List<Usuario> getLista() throws Exception {

        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT id, nome, login, senha FROM usuario ");
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        ResultSet resultSet = preparedStatement.executeQuery();
        List<Usuario> usuarios = new ArrayList<Usuario>();
        while (resultSet.next()) {
            String nome = resultSet.getString("nome");
            String login = resultSet.getString("login");
            String senha = resultSet.getString("senha");
            Long id = resultSet.getLong("id");
            usuarios.add(new Usuario(id, nome, login, senha));

        }
        resultSet.close();
        preparedStatement.close();
        connection.close();
        return usuarios;

    }
}
