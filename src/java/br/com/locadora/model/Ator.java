/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.model;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

/**
 *
 * @author Washington
 */
public class Ator implements Serializable{
    
    private static final long serialVersionUID = 1L;    
    
    private Long id;
    
    private String nome;
    
    //Many-To-Many
    private Set<Filme> filmes;

    public Long getId() {
        return id;
    }

    public String getNome() {
        return nome;
    }

    public Set<Filme> getFilmes() {
        return filmes;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public void setFilmes(Set<Filme> filmes) {
        this.filmes = filmes;
    }

    @Override
    public int hashCode() {
        int hash = 5;
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
        final Ator other = (Ator) obj;
        if (!Objects.equals(this.nome, other.nome)) {
            return false;
        }
        return true;
    }
    
    
    
    
    
    
    
}
