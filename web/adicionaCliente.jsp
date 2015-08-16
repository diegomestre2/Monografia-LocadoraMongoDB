<%-- 
    Document   : adicionaUsuario
    Created on : 26/07/2015, 10:24:57
    Author     : diegogomestome
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <link href="css/bootstrap.min.css" rel="stylesheet">
        <title>Novo Cliente</title>
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
            <div class="row">
                <div class="col-lg-4">
                       
                </div>
                <div class="col-lg-4">
                    <div class="well bs-component">
                        <form id="formCliente" class="form-horizontal" action="clienteMvc" method="post">
                            <input type="hidden" name="logica" value="AdicionaCliente">
                            <input type="hidden" name="id" value="${param.id}">
                            <fieldset>
                                <legend>Novo Cliente</legend>
                                <%
                                    if(request.getAttribute("erro") == "true"){
                                %>    
                                <div class="alert alert-danger" >
                                    <strong><%=request.getAttribute("mensagemErro")%></strong>
                                </div>
                                <%
                                    }    
                                %>        
                                <div class="form-group">
                                    <label for="inputNome" class="col-lg-2 control-label">Nome*</label>
                                    <div class="col-lg-10">
                                        <input type="text" value="${param.nome}" name="nome" class="form-control" id="inputNome" placeholder="Nome">                    
                                    </div>
                                </div>
                                <div class="form-group">
                                    <label for="inputCPF" class="col-lg-2 control-label">Cpf*</label>
                                    <div class="col-lg-10">
                                        <input type="text" value="${param.cpf}" name="cpf" class="form-control" id="inputCPF" placeholder="CPF">
                                    </div>
                                </div>
                                <div class="form-group">
                                    <label for="inputIdade" class="col-lg-2 control-label">Idade*</label>
                                    <div class="col-lg-10">
                                        <input type="text"  value="${param.idade}" name="idade" class="form-control" id="inputIdade" placeholder="Idade">                    
                                    </div>
                                </div>



                                <div class="form-group">
                                    <div class="col-lg-10 col-lg-offset-2">
                                        <button type="reset" form="formCliente" class="btn btn-default" onclick="this.form.reset();">Cancelar</button>
                                        <button type="submit" class="btn btn-primary">Salvar</button>
                                    </div>
                                </div>
                            </fieldset>
                        </form>
                    </div>
                </div>
                <div class="col-lg-4">

                </div>
            </div>
        </div>
        <script src="//ajax.googleapis.com/ajax/libs/jquery/2.0.2/jquery.min.js"></script>
        <script src="js/bootstrap.min.js"></script>
    </body>
</html>
