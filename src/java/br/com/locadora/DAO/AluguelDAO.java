/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.DAO;


import br.com.locadora.exception.AluguelPendenteException;
import br.com.locadora.exception.ClassificacaoIndicativaException;
import br.com.locadora.exception.QuantidadeFilimesException;
import br.com.locadora.model.Aluguel;
import br.com.locadora.model.Cliente;
import br.com.locadora.model.Filme;
import br.com.locadora.model.Usuario;
import br.com.locadora.util.ConexaoUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.Days;

/**
 *
 * @author Washington
 */
public class AluguelDAO {

    FilmeDAO filmeDAO = new FilmeDAO();
    
    public Long obterNovoId(Connection connection) throws Exception{
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT NEXTVAL('sq_aluguel_id') as id ");
        
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql.toString());
        resultSet.next();
        Long id = resultSet.getLong("id");
        
        resultSet.close();
        statement.close();
        
        return id;
    }
    
    public double alugar(Cliente cliente, Usuario usuario, Date dataAluguel, List<Filme> filmes) 
            throws AluguelPendenteException, QuantidadeFilimesException, ClassificacaoIndicativaException, Exception{
        
        //Verificando quantidade de filmes
        if(filmes.size() > 3){
            throw new QuantidadeFilimesException();
        }
        
        if(this.buscarAluguelPendente(cliente) != null){
            throw new AluguelPendenteException();
        }
        
        //Salvando o aluguel para depois salvar os filmes para esse aluguel
        Double valor = 0D;
        for(Filme filme : filmes){
            //Verificando idade do cliente. Passa o filme para a exceção para que saiba qual está fora da idade
            if(filme.getClassificacao() > cliente.getIdade()){
                throw new ClassificacaoIndicativaException(filme);
            }
            valor += filme.getPreco();
        }
        StringBuilder sql = new StringBuilder();
        sql.append(" INSERT INTO aluguel(id, data_aluguel, cliente_id, valor, operador_id) ");
        sql.append(" VALUES (?, ?, ?, ?, ?)");
        
        Connection connection = ConexaoUtil.getConnection();
        Long idAluguel = this.obterNovoId(connection);
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, idAluguel);
        preparedStatement.setTimestamp(2, new Timestamp(dataAluguel.getTime()));
        preparedStatement.setLong(3, cliente.getId());
        preparedStatement.setDouble(4, valor);
        preparedStatement.setLong(5, 1);
        preparedStatement.execute();
        
        sql.setLength(0);
        
        sql.append(" INSERT INTO aluguel_filme (aluguel_id, filme_id) VALUES (?, ?)");
        preparedStatement = connection.prepareStatement(sql.toString());
        for(Filme filme : filmes){
            preparedStatement.setLong(1, idAluguel);
            preparedStatement.setLong(2, filme.getId());
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
        connection.close();
        
        Aluguel aluguelSalvo = new Aluguel(idAluguel, dataAluguel,filmes, cliente, valor, usuario);
        double preco=0;
        for(Filme filme :filmes){
            preco += filme.getPreco();
        }
        return preco;
    }

    public Aluguel buscarAluguelPendente(Cliente cliente) throws Exception{
    
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT a.id idAluguel, data_aluguel, valor, u.id idOperador, u.nome  FROM aluguel a inner join usuario u ON u.id = a.operador_id ") ;
                sql.append(" where cliente_id = ? AND data_devolucao is null ");
        
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(1, cliente.getId());
        ResultSet resultSet = preparedStatement.executeQuery();
        
        Aluguel aluguel = null;
        while(resultSet.next()){
            aluguel = new Aluguel();
            aluguel.setId(resultSet.getLong("idAluguel"));
            aluguel.setDataAluguel(resultSet.getDate("data_aluguel"));
   
            aluguel.setValor(resultSet.getDouble("valor"));
            aluguel.setOperador(new Usuario());
            aluguel.getOperador().setId(resultSet.getLong("idOperador"));
            aluguel.getOperador().setNome(resultSet.getString("nome"));
            
            aluguel.setFilmes(filmeDAO.buscarFilmesPorAluguel(aluguel.getId()));
            
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();
        return aluguel;
    }        
    
    /**
     * Devolve o filme e retorna o valor do pagamento
     * @param cliente
     * @param dataDevolucao
     * @return
     * @throws Exception 
     */
    public double devolucao(Cliente cliente, Date dataDevolucao) throws Exception{
        Aluguel aluguelPendente = this.buscarAluguelPendente(cliente);
        Double valorTotal = this.calcularValorTotal(aluguelPendente, dataDevolucao);
        
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE aluguel set data_devolucao = ? where cliente_id = ? AND data_devolucao is null");
        
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setLong(2, cliente.getId());
        preparedStatement.setTimestamp(1, new Timestamp(dataDevolucao.getTime()));
        preparedStatement.execute();
        preparedStatement.close();
        connection.close();
        
        return valorTotal;
    }

    /**
     * Calcula o valor total do aluguel
     * @param aluguelPendente
     * @param dataDevolucao
     * @return 
     */
    private Double calcularValorTotal(Aluguel aluguelPendente, Date dataDevolucao) {
        int quantFilmes = aluguelPendente.getFilmes().size();
        DateTime dateAluguel = new DateTime(aluguelPendente.getDataAluguel().getTime());
        DateTime dateDevolucao = new DateTime(dataDevolucao);
        int diferencaDias = Days.daysBetween(dateAluguel, dateDevolucao).getDays();
        int diasAtraso = diferencaDias - 2 - (quantFilmes - 1);
        Double multa = 1.2 * (diasAtraso > 0 ? diasAtraso : 0);
        Double valorTotal = aluguelPendente.getValor() + multa;
        return valorTotal;
    }
    
    /**
     * Listar os alugueis por filme
     * @param filme
     * @return
     * @throws Exception 
     */
    public List<Aluguel> listarAlugueisPorFilme(Filme filme) throws Exception{
        
        StringBuilder sql = new StringBuilder();
        sql.append(" SELECT a.id id, a.data_aluguel data_aluguel, a.data_devolucao data_devolucao, a.valor valor, c.nome nome FROM aluguel a "); 
        sql.append(" INNER JOIN aluguel_filme af ON af.aluguel_id = a.id  INNER JOIN filme f ON f.id = af.filme_id ") ;
        sql.append(" INNER JOIN cliente c on c.id = a.cliente_id ") ;
        sql.append(" where f.titulo like ? ");
        sql.append(" GROUP BY a.id, a.data_aluguel, a.data_devolucao, a.valor, c.nome ");
        
        Connection connection = ConexaoUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setString(1, "%" + filme.getTitulo() + "%");
        ResultSet resultSet = preparedStatement.executeQuery();
        
        List<Aluguel> alugueis = new ArrayList<Aluguel>();
        while(resultSet.next()){
            Aluguel aluguel = new Aluguel();
            aluguel.setId(resultSet.getLong("id"));
            aluguel.setDataAluguel(resultSet.getDate("data_aluguel"));
            aluguel.setDataDevolucao(resultSet.getDate("data_devolucao"));
            aluguel.setValor(resultSet.getDouble("valor"));
            Cliente cliente = new Cliente();
            cliente.setNome(resultSet.getString("nome"));
            aluguel.setCliente(cliente);
            alugueis.add(aluguel);
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();
        return alugueis;
    }
    
    
}
