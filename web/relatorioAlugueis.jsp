<%-- 
    Document   : lista-filmes
    Created on : 28/07/2015, 01:32:05
    Author     : diegogomestome
--%>


<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
         <link href="css/bootstrap.min.css" rel="stylesheet">
        <title>Alugueis Filme</title>
    </head>
    <body>
        <div class="navbar-wrapper">

            <c:import url="header.jsp"/>
            <div class="page-header">
                <div class="row">
                </div>
            </div>
            <form class="navbar-form  container" style="margin-left: 100px;" role="search" action="filmeMvc">
                <input type="hidden" name="logica" value="RelatorioAlugueis"/>
                <div class="form-group">
                    <input type="text" name="nome" class="form-control" placeholder="Título do filme">
                </div>
                <button type="submit" class="btn btn-default">Buscar</button>

            </form>
            <div class="container">


                <table class="table table-striped table-hover ">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Data aluguel</th>
                            <th>Data revolução</th>
                            <th>Cliente</th>
                            <th>Valor</th>
                            <th></th>
                            <th></th>
                        </tr>
                    </thead>
                    <tbody>

                        <c:forEach var="aluguel" items="${alugueis}">
                            <tr class="success">
                                <td></td>
                                <td>
                                    ${aluguel.dataAluguel}
                                </td>

                                <td>
                                    ${aluguel.dataDevolucao}
                                </td>

                                <td>
                                    ${aluguel.cliente.nome}
                                </td>
                                
                                <td>
                                    ${aluguel.valor}
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

