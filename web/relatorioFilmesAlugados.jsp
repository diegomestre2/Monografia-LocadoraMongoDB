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
        <title>Lista Filmes</title>
    </head>
    <body>
        <div class="navbar-wrapper">

            <c:import url="header.jsp"/>
            <div class="page-header">
                <div class="row">
                </div>
            </div>
            <form class="navbar-form  container" style="margin-left: 100px;" role="search" action="filmeMvc">
                <input type="hidden" name="logica" value="FilmesAlugados"/>
                <div class="form-group">
                    <input type="text" name="cpf" class="form-control" placeholder="CPF">
                </div>
                <button type="submit" class="btn btn-default">Buscar</button>

            </form>
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
                            <th></th>
                            <th></th>
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

                                <td>
                                    <a href="/Locadora/filmeMvc?logica=RemoverFilme&id=${filme.id}">Remover</a>
                                </td>
                                <td>
                                    <a href="/Locadora/adicionaFilme.jsp?id=${filme.id}&titulo=${filme.titulo}&classificao=${filme.classificacao}&preco=${filme.preco}&genero=${filme.genero}&atores=${filme.atoresFormatado()}">Editar</a>
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

