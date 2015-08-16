/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.logicas;

import br.com.locadora.DAO.AluguelDAO;
import br.com.locadora.DAO.FilmeDAO;
import br.com.locadora.model.Aluguel;
import br.com.locadora.model.Filme;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author diegogomestome
 */
public class ListaAluguelLogica implements Logica {

  @Override
  public String executa(HttpServletRequest req, HttpServletResponse res)
        throws Exception {

      // Monta a lista de contatos
      List<Aluguel> alugueis = null ;

      // Guarda a lista no request
      req.setAttribute("alugueis", alugueis);
  
      return "/listaAluguel.jsp";
  }
}