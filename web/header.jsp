<%-- 
    Document   : header
    Created on : 26/07/2015, 14:48:10
    Author     : diegogomestome
--%>

<div class="container">
    <div class="row">
        <nav class="navbar navbar-default navbar-fixed-top">
            <div class="container-fluid">
                <div class="navbar-header">
                    <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#bs-example-navbar-collapse-1">
                        <span class="sr-only">Toggle navigation</span>
                        <span class="icon-bar"></span>
                        <span class="icon-bar"></span>
                        <span class="icon-bar"></span>
                    </button>
                    <a class="navbar-brand" href="#">Movie-UECE</a>
                </div>

                <div class="collapse navbar-collapse" id="bs-example-navbar-collapse-1">
                    <ul class="nav navbar-nav">
                        <li class="active"><a href="principal.jsp">Inicio <span class="sr-only">(current)</span></a></li>
                        <li> <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false">Clientes <span class="caret"></span></a>
                            <ul class="dropdown-menu" role="menu">
                                <li><a href="/Locadora/adicionaCliente.jsp">Novo</a></li>
                                <li><a href="/Locadora/clienteMvc?logica=ListaClientes">Buscar</a></li>
                               
                            </ul></li>
                        <li class="dropdown">
                            <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false">Filmes <span class="caret"></span></a>
                            <ul class="dropdown-menu" role="menu">
                                <li><a href="/Locadora/adicionaFilme.jsp">Novo</a></li>
                                <li><a href="/Locadora/filmeMvc?logica=ListaFilmes">Buscar</a></li>
                            </ul>
                        </li>
                        
                        <li class="dropdown">
                            <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false">Relatório <span class="caret"></span></a>
                            <ul class="dropdown-menu" role="menu">
                                <li><a href="/Locadora/relatorioFilmesAlugados.jsp">Filmes Alugados</a></li>
                                <li><a href="/Locadora/filmeMvc?logica=RankingFilmes">Ranking Filmes</a></li>
                                <li><a href="/Locadora/relatorioAlugueis.jsp">Alugueis</a></li>
                            </ul>
                        </li>
                    </ul>
                   
                   
                    <ul class="nav navbar-nav navbar-right">
                        <li class="dropdown">
                            <a class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false" href="#">Operador: <%=request.getSession().getAttribute("login") %></a>
                        </li>
                    </ul>
                </div>
            </div>
        </nav>
    </div>
</div>
