/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.logicas;

import br.com.locadora.DAO.AluguelDAO;
import br.com.locadora.DAO.ClienteDAO;
import br.com.locadora.model.Cliente;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author diegogomestome
 */
public class DevolverAluguelLogica implements Logica {

    @Override
    public String executa(HttpServletRequest req, HttpServletResponse res)
            throws Exception {

        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        String cpf = req.getParameter("cpfCliente");
        Date data = sdf.parse(req.getParameter("data"));
        ClienteDAO dao = new ClienteDAO();
        

        Cliente cliente = dao.buscarCliente(cpf);
         AluguelDAO aluguelDao = new AluguelDAO();
         double valor =aluguelDao.devolucao(cliente, data);
req.setAttribute("devolverRetorno", "Devolução efetivada! Valor:" + valor);
        System.out.println("Adicionando usuario... ");

        return "principal.jsp";
    }

}
