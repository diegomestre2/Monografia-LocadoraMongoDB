/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.logicas;

import br.com.locadora.DAO.ClienteDAO;
import br.com.locadora.DAO.mongoDB.ClienteDAOMongo;
import br.com.locadora.model.Cliente;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author diegogomestome
 */
public class RemoverClienteLogica implements Logica {

  @Override
  public String executa(HttpServletRequest req, HttpServletResponse res)
      throws Exception {

    long id = Long.parseLong(req.getParameter("id"));

    Cliente cliente = new Cliente();
    cliente.setId(id);

      ClienteDAO dao = new ClienteDAO();
      ClienteDAOMongo daoMongo = new ClienteDAOMongo();
      daoMongo.excluir(cliente.getId());
    dao.excluir(cliente.getId());

    return "clienteMvc?logica=ListaClientes";
  }

}
