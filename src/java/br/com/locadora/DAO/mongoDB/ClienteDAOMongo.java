/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.DAO.mongoDB;

import br.com.locadora.exception.DuplicateRecordException;
import br.com.locadora.model.Cliente;
import br.com.locadora.util.ConexaoUtil;
import br.com.locadora.util.MongoConnection;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DBCollection;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

/**
 *
 * @author Raimundinha
 */
public class ClienteDAOMongo {

    public void update(Cliente c) throws Exception {
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

    public void inserir(Cliente c) throws Exception {

        MongoConnection connection = MongoConnection.getInstance();
        connection.getDB().getCollection("cliente").insertOne(new Document("_id", c.getId())
                .append("nome", c.getNome())
                .append("cpf", c.getCPF())
                .append("idade", c.getIdade()));

    }

    public Cliente recuperar(Long id) throws Exception {

        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT id, nome, cpf, idade FROM cliente where id = ? ");
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, id);
        ResultSet resultSet = preparedStatement.executeQuery();
        Cliente cliente = null;
        while (resultSet.next()) {
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

    public boolean excluir(Long id) throws Exception {
        MongoConnection connection = MongoConnection.getInstance();
        DeleteResult deleteMany = connection.getDB().getCollection("cliente").deleteMany(new Document("_id", id));
        System.out.print("Deletado no mongo " + deleteMany.getDeletedCount() + " " + id);
        return true;

    }

    public Cliente buscarCliente(String cpf) throws Exception {

        MongoConnection connection = MongoConnection.getInstance();
        FindIterable<Document> find = connection.getDB().getCollection("cliente").find(new Document("cpf", cpf));
        Cliente cliente = new Cliente();
        Document dCliente = find.first();
        String nome = dCliente.getString("nome");
        int idade = dCliente.getInteger("idade");
        Long id = dCliente.getLong("_id");
        cliente = new Cliente(id, nome, cpf, idade);

        return cliente;

    }

    public List<Cliente> buscarClientes(Cliente clienteFiltro) throws Exception {

        MongoConnection connection = MongoConnection.getInstance();
        Document filter = new Document();
        if (clienteFiltro.getNome() != null && !clienteFiltro.getNome().trim().equals("")) {
            filter = new Document("nome", "/" + clienteFiltro.getNome() + "/");
        }
        FindIterable<Document> find = connection.getDB().getCollection("cliente").find(filter);
        final List<Cliente> clientes = new ArrayList<Cliente>();
        find.forEach(new Block<Document>() {
            @Override
            public void apply(final Document dCliente) {
                String nome = dCliente.getString("nome");
                String cpf = dCliente.getString("cpf");
                int idade = dCliente.getInteger("idade");
                Long id = new Long(dCliente.getLong("_id"));
                Cliente cliente = new Cliente(id, nome, cpf, idade);
                clientes.add(cliente);
            }
        });
        return clientes;

    }
}
