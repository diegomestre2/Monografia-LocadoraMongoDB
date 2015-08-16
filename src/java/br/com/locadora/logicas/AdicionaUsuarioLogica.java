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
public class AdicionaUsuarioLogica implements Logica {

    @Override
    public String executa(HttpServletRequest req, HttpServletResponse res)
            throws Exception {


        String nome = req.getParameter("nome");
        String login = req.getParameter("login");
        String senha = req.getParameter("senha");

        Usuario usuario = new Usuario(nome, login, senha);

        UsuarioDAO dao = new UsuarioDAO();
        dao.inserir(usuario);

        System.out.println("Adicionando usuario... ");

        return "usuarioMvc?logica=ListaUsuarios";
    }

}
