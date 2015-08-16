/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.model;

import java.io.Serializable;

/**
 *
 * @author Washington
 */
public class Usuario implements Serializable{
    private static final long serialVersionUID = 1L;
    
    private Long id;
    
    private String nome;
    
    private String login;
    
    private String senha;

    public Usuario(String nome, String login, String senha){
        this.login = login;
        this.senha = senha;
        this.nome = nome;
    }
    public Usuario(Long id,String nome, String login, String senha){
        this.login = login;
        this.senha = senha;
        this.nome = nome;
        this.id = id;
    }

    public Usuario() {}
    
    public Long getId() {
        return id;
    }

    public String getNome() {
        return nome;
    }

    public String getLogin() {
        return login;
    }

    public String getSenha() {
        return senha;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public void setSenha(String senha) {
        this.senha = senha;
    }
    
    
    
    
    
}
