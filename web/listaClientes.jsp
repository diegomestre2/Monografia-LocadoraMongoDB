<%-- 
    Document   : lista-filmes
    Created on : 28/07/2015, 01:32:05
    Author     : diegogomestome
--%>

<%@page import="java.util.List" %> 
<%@page import="br.com.locadora.model.Cliente" %> 
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%
   
   List<Cliente> clientes = (List<Cliente>)request.getAttribute("clientes");
%>
    
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
         <link href="css/bootstrap.min.css" rel="stylesheet">
        <title>Clientes</title>
    </head>
    <body>
        <div class="navbar-wrapper">

            <c:import url="header.jsp"/>
             <div class="page-header">
                <div class="row">
                </div>
            </div>
            <div class="container">
            <table class="table table-striped table-hover ">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Nome</th>
                        <th>CPF</th>
                        <th>Idade</th>
                        <th></th>
                        <th></th>
                    </tr>
                </thead>
                <tbody>

                <c:forEach var="cliente" items="${clientes}">
                    <tr class="success">
                        <td></td>
                        <td>
                            ${cliente.nome}
                        </td>

                        <td>
                            ${cliente.CPF}
                        </td>

                        <td>
                            ${cliente.idade}
                        </td>

                        <td>
                            <a href="/Locadora/clienteMvc?logica=RemoverCliente&id=${cliente.id}">Remover</a>
                        </td>
                        <td>
                            <a href="/Locadora/adicionaCliente.jsp?nome=${cliente.nome}&cpf=${cliente.CPF}&idade=${cliente.idade}&id=${cliente.id}">Editar</a>
                        </td>
                        
                    </tr>
                </c:forEach>
            </tbody>
            </table>
                </div>
        </div>
         <script src="//ajax.googleapis.com/ajax/libs/jquery/2.0.2/jquery.min.js"></script>
         <script src="js/bootstrap.min.js"></script>
             
    </body>
</html>

