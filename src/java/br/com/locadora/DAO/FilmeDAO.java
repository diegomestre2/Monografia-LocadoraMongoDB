/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.DAO;

import br.com.locadora.enums.GeneroEnum;
import br.com.locadora.model.Ator;
import br.com.locadora.model.Cliente;
import br.com.locadora.model.Filme;
import br.com.locadora.model.Usuario;
import br.com.locadora.util.ConexaoUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Washington
 */
public class FilmeDAO {

    AtorDAO atorDAO = new AtorDAO();

      public Long obterNovoId(Connection connection) throws Exception{
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT NEXTVAL('sq_filme_id') as id ");
        
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql.toString());
        resultSet.next();
        Long id = resultSet.getLong("id");
        
        resultSet.close();
        statement.close();
        
        return id;
    }
      
    public void update(Filme filme) throws Exception {
        Connection connection = ConexaoUtil.getConnection();
        StringBuilder sql = new StringBuilder();
        sql.append(" UPDATE filme SET  titulo = ?, genero_enum = ?, classificacao = ?, preco = ? ");
        sql.append(" WHERE id = ? ");
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setString(1, filme.getTitulo());
        preparedStatement.setInt(2, filme.getGenero().ordinal());
        preparedStatement.setInt(3, filme.getClassificacao());
        preparedStatement.setDouble(4, filme.getPreco());
        preparedStatement.setLong(5, filme.getId());
        preparedStatement.execute();
        preparedStatement.close();
        Set<Ator> atoresFilme = atorDAO.buscarAtoresPorFilme(filme.getId());
        Set<Ator> atoresNovos = new HashSet<Ator>(filme.getAtores());
        if(atoresFilme != null){
            atoresNovos.removeAll(atoresFilme);
            atoresFilme.removeAll(filme.getAtores());
            for(Ator ator : atoresFilme){
                atorDAO.excluirAtorFilme(ator.getId(), filme.getId());
            }        
        }
        for(Ator ator : atoresNovos){
                ator = atorDAO.inserir(ator);
                this.salvarRelacaoFilmeAtor(filme, ator, connection);
         }  

        connection.close();

    }

    public void inserir(Filme filme) throws Exception {
        if (filme.getId() != null) {
            this.update(filme);
        } else {
            Connection connection = ConexaoUtil.getConnection();
            StringBuilder sql = new StringBuilder();
            sql.append(" INSERT INTO filme(id, titulo, genero_enum, classificacao, preco) ");
            sql.append(" VALUES ( ?, ?, ?, ?, ?) ");

            PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
            filme.setId(this.obterNovoId(connection));
            preparedStatement.setLong(1, filme.getId());
            preparedStatement.setString(2, filme.getTitulo());
            preparedStatement.setInt(3, filme.getGenero().ordinal());
            preparedStatement.setInt(4, filme.getClassificacao());
            preparedStatement.setDouble(5, filme.getPreco());
            preparedStatement.execute();

            for (Ator ator : filme.getAtores()) {
                ator = atorDAO.inserir(ator);
                this.salvarRelacaoFilmeAtor(filme, ator, connection);
            }

            preparedStatement.close();
            connection.close();
        }

    }

    private void salvarRelacaoFilmeAtor(Filme filme, Ator ator, Connection connection) throws Exception {
        StringBuilder sql = new StringBuilder();
        sql.append(" INSERT INTO filme_ator(filme_id, ator_id) ");
        sql.append(" VALUES ( ?, ?) ");

        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, filme.getId());
        preparedStatement.setLong(2, ator.getId());
        preparedStatement.execute();
        preparedStatement.close();
    }

    public Filme recuperar(Long id) throws Exception {

        StringBuffer sql = new StringBuffer();
        sql.append(" SELECT id, titulo, genero_enum, classificacao, preco FROM filme where id = ? ");

        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, id);
        ResultSet resultSet = preparedStatement.executeQuery();
        Filme filme = null;
        while (resultSet.next()) {
            String nome = resultSet.getString("titulo");
            int genero = resultSet.getInt("genero_enum");
            int classificacao = resultSet.getInt("classificacao");
            Double preco = resultSet.getDouble("preco");
            filme = new Filme(id, nome, GeneroEnum.fromOrdinal(genero), classificacao, preco);
            filme.setAtores(atorDAO.buscarAtoresPorFilme(filme.getId()));
        }

        resultSet.close();
        preparedStatement.close();
        connection.close();
        return filme;
    }

    public boolean excluir(Long id) throws Exception {

        StringBuilder sql = new StringBuilder();
        Connection connection = ConexaoUtil.getConnection();
        sql.append(" DELETE FROM  filme_ator WHERE filme_id = ? ");
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, id);
        preparedStatement.execute();
        
        sql = new StringBuilder();
        sql.append(" DELETE FROM  filme WHERE id = ? ");
        preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, id);
        preparedStatement.execute();
    
        preparedStatement.close();
        connection.close();
        return true;
    }

    public List<Filme> buscarFilme(String titulo) throws Exception {

        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT id, titulo, genero_enum, classificacao, preco FROM filme where titulo ilike ? ");

        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setString(1, "%" + titulo + "%");
        ResultSet resultSet = preparedStatement.executeQuery();
        List<Filme> filmes = new ArrayList<Filme>();
        while (resultSet.next()) {
            String nome = resultSet.getString("titulo");
            int genero = resultSet.getInt("genero_enum");
            int classificacao = resultSet.getInt("classificacao");
            Double preco = resultSet.getDouble("preco");
            Long id = resultSet.getLong("id");
            Filme filme = new Filme(id, nome, GeneroEnum.fromOrdinal(genero), classificacao, preco);
            filme.setAtores(atorDAO.buscarAtoresPorFilme(filme.getId()));
            filmes.add(filme);
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();
        return filmes;
    }

    List<Filme> buscarFilmesPorAluguel(Long idAluguel) throws Exception {
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT f.id id, f.titulo titulo, f.genero_enum genero, f.classificacao classif, f.preco preco FROM filme f ");
        sql.append(" INNER JOIN aluguel_filme af ON af.filme_id = f.id where af.aluguel_id = ? ");

        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, idAluguel);
        ResultSet resultSet = preparedStatement.executeQuery();
        List<Filme> filmes = new ArrayList<Filme>();
        while (resultSet.next()) {
            String nome = resultSet.getString("titulo");
            int genero = resultSet.getInt("genero");
            int classificacao = resultSet.getInt("classif");
            Double preco = resultSet.getDouble("preco");
            Long id = resultSet.getLong("id");
            Filme filme = new Filme(id, nome, GeneroEnum.fromOrdinal(genero), classificacao, preco);
            filme.setAtores(atorDAO.buscarAtoresPorFilme(filme.getId()));
            filmes.add(filme);
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();
        return filmes;

    }

    /**
     * Listar todos os filmes
     * @return
     * @throws SQLException
     * @throws Exception 
     */
    public List<Filme> getLista() throws SQLException, Exception {
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT id, titulo, genero_enum, classificacao, preco FROM filme ");
        List<Filme> filmes = new ArrayList<Filme>();
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        ResultSet resultSet = preparedStatement.executeQuery();
        try {

            while (resultSet.next()) {

                String titulo = resultSet.getString("titulo");
                int genero = resultSet.getInt("genero_enum");
                int classificacao = resultSet.getInt("classificacao");
                Long id = resultSet.getLong("id");
                Double preco = resultSet.getDouble("preco");
                Filme filme = new Filme(id, titulo, GeneroEnum.fromOrdinal(genero), classificacao, preco);
                filme.setAtores(atorDAO.buscarAtoresPorFilme(filme.getId()));
                filmes.add(filme);

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            resultSet.close();
            preparedStatement.close();
            connection.close();
        }
        return filmes;
    }

    public List<Filme> sugerirFilmes(Filme filme) throws Exception{
        filme = this.recuperar(filme.getId());
        String atores = new String();
        for(Ator ator : filme.getAtores()){
            atores += "|" + ator.getNome();
        }
        atores = atores.replaceFirst("|", "");
        
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT f.id id, f.titulo titulo, f.genero_enum genero, f.classificacao classif, f.preco preco FROM filme f ");
            sql.append(" INNER JOIN filme_ator fa ON fa.filme_id = f.id INNER JOIN ator a on a.id = fa.ator_id ");
            sql.append(" where f.genero_enum = ?  ");
            if(!atores.trim().equals("")){
                sql.append(" OR UPPER(a.nome) SIMILAR TO '%(" + atores + ")%'");
            }
            sql.append(" GROUP BY f.id, f.titulo, f.genero_enum, f.classificacao, f.preco ");
        
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setInt(1, filme.getGenero().ordinal()); 
        ResultSet resultSet = preparedStatement.executeQuery();
        List<Filme> filmes = new ArrayList<Filme>();
        while(resultSet.next()){
            String nome = resultSet.getString("titulo");
            int genero = resultSet.getInt("genero");
            int classificacao = resultSet.getInt("classif");
            Double preco = resultSet.getDouble("preco");
            Long id = resultSet.getLong("id");
            Filme filmeSugerido = new Filme(id, nome, GeneroEnum.fromOrdinal(genero), classificacao, preco);
            filmeSugerido.setAtores(atorDAO.buscarAtoresPorFilme(filme.getId()));
            filmes.add(filmeSugerido);
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();
        return filmes;
    }
    
    
    public List<Filme> listarFilmesPorCliente(String cpf) throws Exception{
        
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT f.id id, f.titulo titulo, f.genero_enum genero, f.classificacao classif, f.preco preco FROM filme f ");
            sql.append(" INNER JOIN aluguel_filme af ON af.filme_id = f.id INNER JOIN aluguel a ON a.id = af.aluguel_id ");
            sql.append(" INNER JOIN cliente c on c.id = a.cliente_id ");
            sql.append(" where c.cpf = ? ");
            sql.append(" GROUP BY f.id, f.titulo, f.genero_enum, f.classificacao, f.preco ");
        
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setString(1, cpf); 
        ResultSet resultSet = preparedStatement.executeQuery();
        List<Filme> filmes = new ArrayList<Filme>();
        while(resultSet.next()){
            String nome = resultSet.getString("titulo");
            int genero = resultSet.getInt("genero");
            int classificacao = resultSet.getInt("classif");
            Double preco = resultSet.getDouble("preco");
            Long id = resultSet.getLong("id");
            Filme filme = new Filme(id, nome, GeneroEnum.fromOrdinal(genero), classificacao, preco);
            filme.setAtores(atorDAO.buscarAtoresPorFilme(filme.getId()));
            filmes.add(filme);
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();
        return filmes;    
    }
    
    
    public List<Filme> listarRankingFilmes() throws Exception{
        
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT count(*) ocorrencias, f.id id, f.titulo titulo, f.genero_enum genero, f.classificacao classif, f.preco preco FROM filme f ");
            sql.append(" INNER JOIN aluguel_filme af ON af.filme_id = f.id INNER JOIN aluguel a ON a.id = af.aluguel_id ");
            sql.append(" GROUP BY f.id, f.titulo, f.genero_enum, f.classificacao, f.preco ");
            sql.append(" ORDER BY count(*) DESC ");
        
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        ResultSet resultSet = preparedStatement.executeQuery();
        List<Filme> filmes = new LinkedList<Filme>();
        while(resultSet.next()){
            String nome = resultSet.getString("titulo");
            int genero = resultSet.getInt("genero");
            int classificacao = resultSet.getInt("classif");
            Double preco = resultSet.getDouble("preco");
            Long id = resultSet.getLong("id");
            Filme filme = new Filme(id, nome, GeneroEnum.fromOrdinal(genero), classificacao, preco);
            filme.setAtores(atorDAO.buscarAtoresPorFilme(filme.getId()));
            filmes.add(filme);
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();
        return filmes;    
    } 
    
}
