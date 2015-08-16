/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.logicas;

import br.com.locadora.DAO.UsuarioDAO;
import br.com.locadora.model.Usuario;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author diegogomestome
 */
public class RemoveUsuarioLogica implements Logica {

  @Override
  public String executa(HttpServletRequest req, HttpServletResponse res)
      throws Exception {

    long id = Long.parseLong(req.getParameter("id"));

    Usuario usuario = new Usuario();
    usuario.setId(id);

      UsuarioDAO dao = new UsuarioDAO();
    dao.excluir(usuario.getId());

    System.out.println("Excluindo usuario... ");

    return "usuarioMvc?logica=ListaUsuarios";
  }

}
