/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.logicas;

import br.com.locadora.DAO.FilmeDAO;
import br.com.locadora.model.Filme;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author diegogomestome
 */
public class RemoverFilmeLogica implements Logica {

  @Override
  public String executa(HttpServletRequest req, HttpServletResponse res)
      throws Exception {

    long id = Long.parseLong(req.getParameter("id"));

    Filme filme = new Filme();
    filme.setId(id);

      FilmeDAO dao = new FilmeDAO();
    dao.excluir(filme.getId());

    return "filmeMvc?logica=ListaFilmes";
  }

}
