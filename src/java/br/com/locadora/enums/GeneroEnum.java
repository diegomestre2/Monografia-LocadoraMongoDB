/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.enums;

/**
 *
 * @author Washington
 */
public enum GeneroEnum {
    ACAO, ADULTO, ANIMACAO, AVENTURA, COMEDIA, DANCA, 
    DOCUMENTARIO, DRAMA, ROMANCE, SUSPENSE, TERROR;

    public static GeneroEnum fromOrdinal(int ordinal){
        switch(ordinal){
            case 0 : return ACAO;
            case 1 : return ADULTO;
            case 2 : return ANIMACAO;
            case 3 : return AVENTURA;
            case 4 : return COMEDIA;
            case 5 : return DANCA;
            case 6 : return DOCUMENTARIO;
            case 7 : return DRAMA;
            case 8 : return ROMANCE;
            case 9 : return SUSPENSE;
            case 10 : return TERROR;
            default : return null;
        } 
    }   
    
    @Override
    public String toString(){
        switch(this){
            case ACAO           : return "Ação";
            case ADULTO         : return "Adulto";
            case ANIMACAO       : return "Animação";    
            case AVENTURA       : return "Aventura";    
            case COMEDIA        : return "Comédia";
            case DANCA          : return "Dança";
            case DOCUMENTARIO   : return "Documentário"; 
            case DRAMA          : return "Drama";
            case ROMANCE        : return "Romance";
            case SUSPENSE       : return "Suspense";
            case TERROR         : return "Terror";
            default             : return "";
        }
    }
    
    
}
