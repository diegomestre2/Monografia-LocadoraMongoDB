/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.logicas;

import br.com.locadora.DAO.ClienteDAO;
import br.com.locadora.DAO.UsuarioDAO;
import br.com.locadora.DAO.mongoDB.ClienteDAOMongo;
import br.com.locadora.exception.DuplicateRecordException;
import br.com.locadora.model.Cliente;
import br.com.locadora.model.Usuario;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author diegogomestome
 */
public class AdicionaClienteLogica implements Logica {

    @Override
    public String executa(HttpServletRequest req, HttpServletResponse res)
            throws Exception {


        String nome = req.getParameter("nome");
        String cpf = req.getParameter("cpf");
        Integer idade = null;
        if(!req.getParameter("idade").trim().equals("")){
            idade = Integer.parseInt(req.getParameter("idade"));
        }
        
        
        if(nome == null || cpf == null || idade == null){
             req.setAttribute("erro", "true");
            req.setAttribute("mensagemErro", "Preencher campos obrigatórios");
            return "/adicionaCliente.jsp";
        }
        Long id = null;
        if(req.getParameter("id") != null && !req.getParameter("id").trim().equals("")){
            id = Long.parseLong(req.getParameter("id"));
        }
        Cliente cliente = new Cliente(id ,nome,cpf,idade);

        ClienteDAO dao = new ClienteDAO();
        ClienteDAOMongo daoMongo = new ClienteDAOMongo();
        try{
            dao.inserir(cliente);
            daoMongo.inserir(dao.buscarCliente(cliente.getCPF()));
        }catch(DuplicateRecordException d){
            req.setAttribute("erro", "true");
            req.setAttribute("mensagemErro", "CPF já cadastrado");
            return "/adicionaCliente.jsp";
        }
        return "clienteMvc?logica=ListaClientes";
    }

}