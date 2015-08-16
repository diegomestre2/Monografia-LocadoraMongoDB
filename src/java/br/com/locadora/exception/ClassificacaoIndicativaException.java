/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.exception;

import br.com.locadora.model.Filme;

/**
 *
 * @author Washington
 */
public class ClassificacaoIndicativaException extends Exception{
    Filme filme;

    public ClassificacaoIndicativaException(Filme filme) {
        this.filme = filme;
    }
    
    
}
