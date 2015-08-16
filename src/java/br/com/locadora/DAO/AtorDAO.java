/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.DAO;

import br.com.locadora.enums.GeneroEnum;
import br.com.locadora.model.Ator;
import br.com.locadora.model.Filme;
import br.com.locadora.util.ConexaoUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Washington
 */
public class AtorDAO {

    public Ator inserir(Ator ator) throws Exception {
        Ator atorExistente = new Ator();
        atorExistente = this.buscarPorNome(ator.getNome());
        if(atorExistente != null){
            return atorExistente;
        }
        else{
            Connection connection = ConexaoUtil.getConnection();
            
            Long novoId = this.obterNovoId(connection);
            
            StringBuilder sql = new StringBuilder();
            sql.append(" INSERT INTO ator(id, nome) ");
            sql.append(" VALUES ( ?, ?) ");
            PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.setLong(1, novoId);
            preparedStatement.setString(2, ator.getNome().trim().toUpperCase());
            
            ator.setId(novoId);
            
            preparedStatement.execute();
            preparedStatement.close();
            connection.close();
            return ator;
        }
    }
        
    public Long obterNovoId(Connection connection) throws Exception{
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT NEXTVAL('sq_ator_id') as id ");
        
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql.toString());
        resultSet.next();
        Long id = resultSet.getLong("id");
        
        resultSet.close();
        statement.close();
        
        return id;
    }
    
    public Ator buscarPorNome(String nome) throws Exception{
        
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT id, nome FROM ator where nome = ? ");
        
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setString(1, nome.trim().toUpperCase());
        ResultSet resultSet = preparedStatement.executeQuery();
        Ator ator = new Ator();
        while(resultSet.next()){
            ator.setId(resultSet.getLong("id"));
            ator.setNome(resultSet.getString("nome"));
        }
        
        resultSet.close();
        preparedStatement.close();
        connection.close();
        return ator;
    }
    
    public Ator update(Ator ator) throws Exception {
        Connection connection = ConexaoUtil.getConnection();

        Long novoId = this.obterNovoId(connection);

        StringBuilder sql = new StringBuilder();
        sql.append(" UPDATE ator SET  nome = ? ");
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setString(1, ator.getNome().trim().toUpperCase());
        
        preparedStatement.execute();
        preparedStatement.close();
        connection.close();
        return ator;
    
    }

    Set<Ator> buscarAtoresPorFilme(Long filmeId) throws Exception {
        
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT a.id, a.nome FROM ator a INNER JOIN filme_ator fa on fa.ator_id = a.id ")
                .append("where fa.filme_id = ? ");
        
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, filmeId);
        ResultSet resultSet = preparedStatement.executeQuery();
        Set<Ator> atores = new HashSet<Ator>();
        while(resultSet.next()){
            Ator ator = new Ator();
            ator.setId(resultSet.getLong("id"));
            ator.setNome(resultSet.getString("nome"));
            atores.add(ator);
        }
        
        resultSet.close();
        preparedStatement.close();
        connection.close();
        return atores;
    }
    
     public boolean excluir(Long id) throws Exception {

        StringBuilder sql = new StringBuilder();
        sql.append(" DELETE FROM  ator WHERE id = ? ");

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
     
      public boolean excluirAtorFilme(Long idAtor, Long idFilme) throws Exception {

        StringBuilder sql = new StringBuilder();
        sql.append(" DELETE FROM  filme_ator WHERE ator_id = ? AND filme_id = ? ");

        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, idAtor);
        preparedStatement.setLong(2, idFilme);
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
    

}
