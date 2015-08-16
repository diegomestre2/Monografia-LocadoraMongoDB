/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.model;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 *
 * @author Washington
 */
public class Cliente implements Serializable{
    
    private static final long serialVersionUID = 1L;
    
    private Long id;
    
    private String nome;
    
    private String CPF;
    
    private int idade;
    
    //One-To-Many
    private List<Aluguel> alugueis;
    
    public Cliente(){}

    
    public Cliente(Long id, String nome, String CPF, int idade) {
        this.id = id;
        this.nome = nome;
        this.CPF = CPF;
        this.idade = idade;
    }
    
    

    public String getNome() {
        return nome;
    }

    public String getCPF() {
        return CPF;
    }

    public int getIdade() {
        return idade;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public void setCPF(String CPF) {
        this.CPF = CPF;
    }

    public void setIdade(int idade) {
        this.idade = idade;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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
        final Cliente other = (Cliente) obj;
        if (!Objects.equals(this.CPF, other.CPF)) {
            return false;
        }
        return true;
    }

    public List<Aluguel> getAlugueis() {
        return alugueis;
    }

    public void setAlugueis(List<Aluguel> alugueis) {
        this.alugueis = alugueis;
    }
    
    
    
    
    
    
    
}

