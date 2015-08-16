/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.model;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Washington
 */
public class Aluguel  implements Serializable{
    
    private static final long serialVersionUID = 1L;    
        
    private Long id;
    
    private Date dataAluguel;
    
    private Date dataDevolucao;
    
    //Many-To-Many
    private List<Filme> filmes;
    
    //Many-To-One
    private Cliente cliente;
    
    private Double valor;
    
    //Many-To-One
    private Usuario operador;

    public Aluguel(){
        super();
    }
    
    public Aluguel(Long id, Date dataAluguel, List<Filme> filmes, Cliente cliente, Double valor, Usuario operador) {
        this.id = id;
        this.dataAluguel = dataAluguel;
        this.filmes = filmes;
        this.cliente = cliente;
        this.valor = valor;
        this.operador = operador;
    }
    
    
    

    public Long getId() {
        return id;
    }

    public Date getDataAluguel() {
        return dataAluguel;
    }

    public Date getDataDevolucao() {
        return dataDevolucao;
    }

    public List<Filme> getFilmes() {
        return filmes;
    }

    public Cliente getCliente() {
        return cliente;
    }

    public Double getValor() {
        return valor;
    }

    public Usuario getOperador() {
        return operador;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setDataAluguel(Date dataAluguel) {
        this.dataAluguel = dataAluguel;
    }

    public void setDataDevolucao(Date dataDevolucao) {
        this.dataDevolucao = dataDevolucao;
    }

    public void setFilmes(List<Filme> filmes) {
        this.filmes = filmes;
    }

    public void setCliente(Cliente cliente) {
        this.cliente = cliente;
    }

    public void setValor(Double valor) {
        this.valor = valor;
    }

    public void setOperador(Usuario operador) {
        this.operador = operador;
    }
    
    
    
}
