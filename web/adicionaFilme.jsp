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
            <div class="row col-lg-12">

            </div>
            <div class="row col-lg-12">

            </div>
            <div class="row">
                <div class="col-lg-4">

                </div>
                <div class="col-lg-4">
                    <div class="well bs-component">
                        <form class="form-horizontal" action="filmeMvc" method="post">
                            <input type="hidden" name="logica" value="AdicionaFilme">
                            <input type="hidden" name="id" value="${param.id}">

                            <fieldset>
                                <legend>Novo Filme</legend>
                                <div class="form-group">
                                    <label for="inputTitulo" class="col-lg-2 control-label">Titulo</label>
                                    <div class="col-lg-10">
                                        <input type="text" value="${param.titulo}" name="titulo" class="form-control" id="inputTitulo" placeholder="Titulo">                    
                                    </div>
                                </div>
                                <div class="form-group">
                                    <label for="inputAtores" class="col-lg-2 control-label">Atores</label>
                                    <div class="col-lg-10">
                                        <textarea  placeholder="Atores (separados por vírgula)" name="atores" id="inputAtores" class="form-control" rows="3">${param.atores}</textarea>
                                    </div>
                                </div>
                                <div class="form-group">
                                    <label for="generoSelect" class="col-lg-2 control-label">Gênero</label>
                                    <div class="col-lg-10">
                                        <select  name="genero"  class="form-control" id="generoSelect">
                                            <option ${param.genero == 'ACAO' ? 'selected' : '' }          >1 - ACAO</option>
                                            <option ${param.genero == 'ADULTO' ? 'selected' : '' }        >2 - ADULTO</option>
                                            <option ${param.genero == 'ANIMACAO' ? 'selected' : '' }      >3 - ANIMACAO</option>
                                            <option ${param.genero == 'AVENTURA' ? 'selected' : '' }      >4 - AVENTURA</option>
                                            <option ${param.genero == 'COMEDIA' ? 'selected' : '' }       >5 - COMEDIA</option>
                                            <option ${param.genero == 'DANCA' ? 'selected' : '' }         >6 - DANCA</option>
                                            <option ${param.genero == 'DOCUMENTARIO' ? 'selected' : '' }  >7 - DOCUMENTARIO</option>
                                            <option ${param.genero == 'DRAMA' ? 'selected' : '' }         >8 - DRAMA</option>
                                            <option ${param.genero == 'ROMANCE' ? 'selected' : '' }       >9 - ROMANCE</option>
                                            <option ${param.genero == 'SUSPENSE' ? 'selected' : '' }      >10 - SUSPENSE</option>
                                            <option ${param.genero == 'TERROR' ? 'selected' : '' }        >11 - TERROR</option>
                                        </select>
                                    </div>
                                </div>



                                <div class="form-group">
                                    <label for="classificacao" class="col-lg-3 control-label">Classificacão</label>
                                    <div class="col-lg-9 right">
                                        <select  name="classificacao"  class="form-control" id="classificacao">
                                            <option ${param.classificao == '0' ? 'selected' : '' }>0 anos</option>
                                            <option ${param.classificao == '12' ? 'selected' : '' }>12 anos</option>
                                            <option ${param.classificao == '14' ? 'selected' : '' }>14 anos</option>
                                            <option ${param.classificao == '16' ? 'selected' : '' }>16 anos</option>
                                            <option ${param.classificao == '18' ? 'selected' : '' }>18 anos</option>
                                        </select>
                                    </div>
                                </div>

                                <div class="form-group">
                                    <label for="inputPreco" class="col-lg-2 control-label">Preço</label>
                                    <div class="col-lg-10">
                                        <input type="text" value="${param.preco}" name="preco" class="form-control" id="inputPreco" placeholder="Preço">                    
                                    </div>
                                </div>

                                <div class="form-group">
                                    <div class="col-lg-12 col-lg-offset-6">
                                        <button type="reset" class="btn btn-danger">Cancelar</button>
                                        <button type="submit" class="btn btn-info">Salvar</button>
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
