/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.DAO;

import br.com.locadora.exception.DuplicateRecordException;
import br.com.locadora.model.Cliente;
import br.com.locadora.util.ConexaoUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


/**
 *
 * @author Washington
 */
public class ClienteDAO {
        
    public void update(Cliente  c) throws Exception{
        Connection connection = ConexaoUtil.getConnection();
        StringBuilder sql = new StringBuilder();
        sql.append(" UPDATE cliente SET  nome = ?, cpf = ?, idade = ? ");
        sql.append(" WHERE id = ? ");
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setString(1, c.getNome());
        preparedStatement.setString(2, c.getCPF());
        preparedStatement.setInt(3, c.getIdade());
        preparedStatement.setLong(4, c.getId());
        
        preparedStatement.execute();
        
        preparedStatement.close();
        connection.close();
    }
    
    public void inserir(Cliente c) throws Exception{
        if(c.getId() != null){
            this.update(c);
        }
        else{
            if(this.buscarCliente(c.getCPF()) != null){
                throw new DuplicateRecordException();
            }
            Connection connection = ConexaoUtil.getConnection();
            StringBuilder sql = new StringBuilder();
            sql.append(" INSERT INTO cliente(id, nome, cpf, idade) ");
            sql.append(" VALUES ( NEXTVAL('sq_cliente_id'), ?, ?, ?) ");

            PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.setString(1, c.getNome());
            preparedStatement.setString(2, c.getCPF());
            preparedStatement.setInt(3, c.getIdade());
            preparedStatement.execute();
            
            preparedStatement.close();
            connection.close();
        }
        

    }
    
    
    public Cliente recuperar(Long id) throws Exception{
        
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT id, nome, cpf, idade FROM cliente where id = ? ");
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, id);
        ResultSet resultSet = preparedStatement.executeQuery();
        Cliente cliente = null;
        while(resultSet.next()){
            String nome = resultSet.getString("nome");
            String cpf = resultSet.getString("cpf");
            int idade = resultSet.getInt("idade");
            cliente = new Cliente(id, nome, cpf, idade);
            cliente.setId(resultSet.getLong("id"));
        }
        
        resultSet.close();
        preparedStatement.close();
        connection.close();
        return cliente;
    }
    
    public boolean excluir(Long id) throws Exception{
        
        StringBuilder sql = new StringBuilder();
        sql.append(" DELETE FROM  cliente WHERE id = ? ");
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, id);
        try{
            preparedStatement.execute();
            return true;
        }catch(Exception e){
            return false;
        }finally{
            preparedStatement.close();
            connection.close();
        }
        
    }
    
    public Cliente buscarCliente(String cpf) throws Exception{
        
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT id, nome, cpf, idade FROM cliente where cpf = ? ");
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setString(1, cpf);
        ResultSet resultSet = preparedStatement.executeQuery();
        Cliente cliente = null;
        while(resultSet.next()){
            String nome = resultSet.getString("nome");
            int idade = resultSet.getInt("idade");
            Long id  = resultSet.getLong("id");
            cliente = new Cliente(id, nome, cpf, idade);
        }
        
        resultSet.close();
        preparedStatement.close();
        connection.close();
        return cliente;
    
    }
    
    public List<Cliente> buscarClientes(Cliente clienteFiltro) throws Exception{
        
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT id, nome, cpf, idade FROM cliente ");
        
        if(clienteFiltro.getNome() != null && !clienteFiltro.getNome().trim().equals("")){
            sql.append(" where nome like ? ");
        }
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        if(clienteFiltro.getNome() != null && !clienteFiltro.getNome().trim().equals("")){
            preparedStatement.setString(1, "%" + clienteFiltro.getNome() + "%");
        }
        ResultSet resultSet = preparedStatement.executeQuery();
        List<Cliente> clientes = new ArrayList<Cliente>();
        while(resultSet.next()){
            String nomeCliente = resultSet.getString("nome");
            int idade = resultSet.getInt("idade");
            Long id  = resultSet.getLong("id");
            String cpf = resultSet.getString("cpf");
            Cliente cliente = new Cliente(id, nomeCliente, cpf, idade);
            clientes.add(cliente);
        }
        
        resultSet.close();
        preparedStatement.close();
        connection.close();
        return clientes;
    
    }
}
