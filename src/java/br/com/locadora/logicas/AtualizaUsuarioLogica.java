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
public class AtualizaUsuarioLogica implements Logica {
    
    @Override
    public String executa(HttpServletRequest req, HttpServletResponse res)
            throws Exception {
        
        Long id = Long.getLong(req.getParameter("id"));
        String nome = req.getParameter("nome");
        String login = req.getParameter("login");
        String senha = req.getParameter("senha");
        
        UsuarioDAO dao = new UsuarioDAO();
        Usuario usuario = dao.recuperar(id);
        if (usuario != null) {
            usuario.setLogin(login);
            usuario.setNome(nome);
            usuario.setSenha(senha);
            dao.update(usuario);            
        }
        
        System.out.println("Atualizando usuario... ");
        
        return "usuarioMvc?logica=ListaUsuariossLogica";
    }
    
}
