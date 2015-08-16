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
        <title>Ranking Filmes</title>
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
                            <th>Título</th>
                            <th>Gênero</th>
                            <th>Atores</th>
                            <th>Classificação</th>
                            <th>Preço</th>
                           
                        </tr>
                    </thead>
                    <tbody>

                        <c:forEach var="filme" items="${filmes}">
                            <tr class="success">
                                <td></td>
                                <td>
                                    ${filme.titulo}
                                </td>

                                <td>
                                    ${filme.genero.toString()}
                                </td>

                                <td>
                                    ${filme.atoresFormatado()}
                                </td>
                                
                                <td>
                                    ${filme.classificacao}
                                </td>
                                
                                
                                <td>
                                    ${filme.preco}
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

