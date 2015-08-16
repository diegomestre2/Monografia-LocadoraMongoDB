/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.logicas;

import br.com.locadora.DAO.UsuarioDAO;
import br.com.locadora.model.Usuario;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author diegogomestome
 */
public class ListaUsuariosLogica implements Logica {

  @Override
  public String executa(HttpServletRequest req, HttpServletResponse res)
        throws Exception {

      // Monta a lista de contatos
      List<Usuario> usuarios = new UsuarioDAO().getLista();

      // Guarda a lista no request
      req.setAttribute("usuarios", usuarios);
      System.out.println("Listando usuario... ");
  
      return "/lista-usuarios.jsp";
  }
}