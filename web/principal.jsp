<%-- 
    Document   : principal.jsp
    Created on : 25/07/2015, 20:40:26
    Author     : diegogomestome
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
        <link href="css/bootstrap.min.css" rel="stylesheet"/>
        <link href="css/jquery-ui.css" rel="stylesheet"/>
        <link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/bootstrap-datepicker/1.3.0/css/datepicker.min.css" />
        <link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/bootstrap-datepicker/1.3.0/css/datepicker3.min.css" />

        <script src="//cdnjs.cloudflare.com/ajax/libs/bootstrap-datepicker/1.3.0/js/bootstrap-datepicker.min.js"></script>
        <script src="//ajax.googleapis.com/ajax/libs/jquery/2.0.2/jquery.min.js"></script>
        <script src="js/bootstrap.min.js"></script>
        <script src="js/jquery-ui.js"></script>

        <title>Principal</title>
    </head>
    <body>

        <div class="navbar-wrapper">

            <c:import url="header.jsp"/>



            <div class="page-header">
                <div class="row">
                    <div class="col-lg-12">

                    </div>
                </div>
            </div>
            <div class="container">
                <div class="row">
                    <div class="col-lg-2">

                    </div>
                    <div class="col-lg-8">
                        <button class="btn btn-info col-lg-4" type="button" data-toggle="collapse" data-target="#collapseExample1" aria-expanded="false" aria-controls="collapseExample1">
                            Realizar Aluguel
                        </button>
                        <button class="btn btn-info col-lg-4" type="button" data-toggle="collapse" data-target="#collapseExample2" aria-expanded="false" aria-controls="collapseExample2">
                            Realizar Devolução
                        </button>
                        <button class="btn btn-info col-lg-4" type="button" data-toggle="collapse" data-target="#collapseExample3" aria-expanded="false" aria-controls="collapseExample3">
                            Sugerir Filme
                        </button>
                        <div class="collapse" id="collapseExample1">
                            <div class="well">
                                <c:import url="aluga.jsp"/>
                            </div>
                        </div>
                        <div class="collapse" id="collapseExample2">
                            <div class="well">
                                <c:import url="devolucao.jsp"/>
                            </div>
                        </div>
                        <div class="collapse" id="collapseExample3">
                            <div class="well">
                                <c:import url="sugestao.jsp"/>
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-2">

                    </div>
                </div>
            </div>
            <div class="container">
                <div class="row">
                    <%
                        if (request.getAttribute("retorno") != null) {
                    %>
                    <div class="alert alert-danger" role="alert">${retorno}</div>
                    <%}%>
                </div>
                <div class="row">
                    <%
                        if (request.getAttribute("aluguelRetorno") != null) {
                    %>
                    <div class="alert alert-success">
                        <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a>
                        <strong>#</strong>${aluguelRetorno}
                    </div>
                    <%}%>
                </div>

                  <div class="row">
                    <%
                        if (request.getAttribute("devolverRetorno") != null) {
                    %>
                    <div class="alert alert-success">
                        <a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a>
                        <strong>#</strong>${devolverRetorno}
                    </div>
                    <%}%>
                </div>   
            </div>
                
            <%
                if (request.getAttribute("exibirSugestao") == "true") {
            %>
           

            <div class="well bs-component" style="margin-top: 25px; margin-left: 75px; margin-right: 75px;">


                <fieldset>
                    <legend>Sugestão de filmes</legend>


                    <div class="container">


                        <table class="table table-striped table-hover ">
                            <thead>
                                <tr>
                                    <th>#</th>
                                    <th>Código</th>
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
                                            ${filme.id}
                                        </td>
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



                </fieldset>

            </div>
            <%}%>
    


    </body>
</html>
