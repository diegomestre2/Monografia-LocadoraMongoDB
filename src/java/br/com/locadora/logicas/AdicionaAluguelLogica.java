/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.locadora.logicas;

import br.com.locadora.DAO.AluguelDAO;
import br.com.locadora.DAO.ClienteDAO;
import br.com.locadora.DAO.FilmeDAO;
import br.com.locadora.DAO.UsuarioDAO;
import br.com.locadora.DAO.mongoDB.ClienteDAOMongo;
import br.com.locadora.exception.AluguelPendenteException;
import br.com.locadora.exception.ClassificacaoIndicativaException;
import br.com.locadora.exception.QuantidadeFilimesException;
import br.com.locadora.model.Cliente;
import br.com.locadora.model.Filme;
import br.com.locadora.model.Usuario;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author diegogomestome
 */
public class AdicionaAluguelLogica implements Logica {

    @Override
    public String executa(HttpServletRequest req, HttpServletResponse res)
            throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        String cpf = req.getParameter("cpfCliente");
        Date data = sdf.parse(req.getParameter("data"));
        String idFilme1 = req.getParameter("filme1");
        String idFilme2 = req.getParameter("filme2");
        String idFilme3 = req.getParameter("filme3");

        List<Filme> filmes = new ArrayList<Filme>();

        FilmeDAO filmeDao = new FilmeDAO();
        filmes.add(filmeDao.recuperar(new Long(idFilme1)));
        System.out.print(idFilme2);
        if (idFilme2 != null && !"".equals(idFilme2)) {
            filmes.add(filmeDao.recuperar(new Long(idFilme2)));
        }
        if (idFilme3 != null && !"".equals(idFilme3)) {
            filmes.add(filmeDao.recuperar(new Long(idFilme3)));
        }
        UsuarioDAO usuarioDao = new UsuarioDAO();

        ClienteDAO dao = new ClienteDAO();
        ClienteDAOMongo daoMongo = new ClienteDAOMongo();

        Cliente cliente = daoMongo.buscarCliente(cpf);
        Usuario usuario = usuarioDao.buscarUsuarioPorLogin((String) req.getSession().getAttribute("login"));
        double valor = 0;
        AluguelDAO aluguelDao = new AluguelDAO();
        try {
            valor = aluguelDao.alugar(cliente, usuario, data, filmes);
        } catch (AluguelPendenteException e) {
            req.setAttribute("retorno", "Cliente com aluguel pendente");
            return "principal.jsp";
        } catch (QuantidadeFilimesException e) {
            req.setAttribute("retorno", "Quantidade de filmes ultrapassou o limite de 3");
            return "principal.jsp";
        } catch (ClassificacaoIndicativaException e) {
            req.setAttribute("retorno", "Classificação do filme acima da idade");
            return "principal.jsp";
        }
        req.setAttribute("aluguelRetorno", "Aluguel efetivado! Valor:" + valor);
        return "principal.jsp";
    }

}
