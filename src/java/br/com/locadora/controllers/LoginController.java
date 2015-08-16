package br.com.locadora.controllers;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import br.com.locadora.DAO.ClienteDAO;
import br.com.locadora.DAO.FilmeDAO;
import br.com.locadora.DAO.UsuarioDAO;
import br.com.locadora.model.Cliente;
import br.com.locadora.model.Filme;
import br.com.locadora.model.Usuario;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 *
 * @author diegogomestome
 */
public class LoginController extends HttpServlet {

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        HttpSession session = request.getSession(); //obtem a sessao do usuario, caso exista

        Usuario user = null;
        List<Cliente> clientes = new ArrayList<Cliente>();
        List<Filme> filmes = new ArrayList<Filme>();
        String login_form = request.getParameter("username"); // Pega o Login vindo do formulario
        String senha_form = request.getParameter("password"); //Pega a senha vinda do formulario
        System.out.print(login_form + senha_form);
        try {
            UsuarioDAO dao = new UsuarioDAO(); //cria uma instancia do DAO usuario
            user = dao.buscarUsuario(login_form, senha_form);

            clientes = new ClienteDAO().buscarClientes(new Cliente());
            filmes = new FilmeDAO().getLista();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //se nao encontrou usuario no banco, redireciona para a pagina de erro!
        if (user == null) {
            session.invalidate();
            request.setAttribute("error", "true");
            request.getRequestDispatcher("login.jsp").forward(request, response);
            return;
        } else {

            //se o dao retornar um usuario, coloca o mesmo na sessao
            session.setAttribute("user", user);
            session.setAttribute("login", user.getLogin());
            request.setAttribute("user", user);
            request.setAttribute("clientes", clientes);
            request.setAttribute("filmes", filmes);
            request.getRequestDispatcher("principal.jsp").forward(request, response);
            return;
        }
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}
