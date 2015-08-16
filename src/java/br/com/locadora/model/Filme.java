/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.model;

import br.com.locadora.enums.GeneroEnum;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 *
 * @author Washington
 */
public class Filme implements Serializable{
    
    private static final long serialVersionUID = 1L;
    
    private Long id;
    
    private String titulo;
    
    
    //Many-To-Many
    private Set<Ator> atores = new HashSet<Ator>();
    
    private GeneroEnum genero;
    
    private int classificacao;
    
    private Double preco;
    
    //Many-To-Many
    private List<Aluguel> alugueis;

    
    public Filme(){}

    public Filme(Long id, String titulo, GeneroEnum genero, int classificacao, Double preco) {
        this.id = id;
        this.titulo = titulo;
        this.genero = genero;
        this.classificacao = classificacao;
        this.preco = preco;
    }
    
    public String atoresFormatado(){
        String atores = new String();
        if(!this.atores.isEmpty()){
            for(Ator ator : this.atores){
                atores += ", " + ator.getNome();
            }
        }
        atores =  atores.replaceFirst(",", "");
        return atores.replaceFirst(" ", "");
    }
    
    
    public String getTitulo() {
        return titulo;
    }

    public Set<Ator> getAtores() {
        return atores;
    }

    public List<Aluguel> getAlugueis() {
        return alugueis;
    }

    

    public GeneroEnum getGenero() {
        return genero;
    }

    public int getClassificacao() {
        return classificacao;
    }

    public Double getPreco() {
        return preco;
    }

    public void setTitulo(String titulo) {
        this.titulo = titulo;
    }

    public void setAtores(Set<Ator> atores) {
        this.atores = atores;
    }

    public void setAlugueis(List<Aluguel> alugueis) {
        this.alugueis = alugueis;
    }

    

    public void setGenero(GeneroEnum genero) {
        this.genero = genero;
    }

    public void setClassificacao(int classificacao) {
        this.classificacao = classificacao;
    }

    public void setPreco(Double preco) {
        this.preco = preco;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Filme other = (Filme) obj;
        if (!Objects.equals(this.titulo, other.titulo)) {
            return false;
        }
        return true;
    }
    
    
    
    
    
}
