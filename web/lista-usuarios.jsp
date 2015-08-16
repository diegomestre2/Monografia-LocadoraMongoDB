<%-- 
    Document   : lista-usuarios
    Created on : 26/07/2015, 10:33:59
    Author     : diegogomestome
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Lista Usuarios</title>
    </head>
    <body>
        <table class="table table-striped table-hover ">
            <thead>
                <tr>
                    <th>#</th>
                    <th>Nome</th>
                    <th>Login</th>
                    <th>Senha</th>
                    <th></th>
                    <th></th>
                </tr>
            </thead>
            <tbody>

            <c:forEach var="usuario" items="${usuarios}">
                <tr class="success">
                    <td></td>
                    <td>
                        ${usuario.nome}
                    </td>

                    <td>
                        ${usuario.login}
                    </td>

                    <td>
                        ${usuario.senha}
                    </td>

                    <td>
                        <a href="/Locadora/usuarioMvc?logica=RemoveUsuario&id=${usuario.id}">Remover</a>
                    </td>
                    <td>
                        <a href="/Locadora/atualizaUsuario.jsp&id=${usuario.id}&nome=${usuario.nome}&login=${usuario.login}&senha=${usuario.senha}">Editar</a>
                    </td>
                </tr>
            </c:forEach>
        </tbody>
    </table>
</body>
</html>
