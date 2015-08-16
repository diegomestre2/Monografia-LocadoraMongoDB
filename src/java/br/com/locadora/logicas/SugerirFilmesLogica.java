/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.logicas;

import br.com.locadora.DAO.FilmeDAO;
import br.com.locadora.DAO.UsuarioDAO;
import br.com.locadora.model.Filme;
import br.com.locadora.model.Usuario;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author diegogomestome
 */
public class SugerirFilmesLogica implements Logica {

  @Override
  public String executa(HttpServletRequest req, HttpServletResponse res)
        throws Exception {

      // Monta a lista de contatos
      List<Filme> filmes = new ArrayList<Filme>();
      Filme filme = new Filme();
      
      if(req.getParameter("codigo") != null && !req.getParameter("codigo").equals("") ){
          filme.setId(Long.parseLong(req.getParameter("codigo")));
          filmes = new FilmeDAO().sugerirFilmes(filme);
      }
      // Guarda a lista no request
      req.setAttribute("filmes", filmes);
      req.setAttribute("exibirSugestao", "true");
      
  
      return "/principal.jsp";
  }
}