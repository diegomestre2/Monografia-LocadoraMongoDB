<!DOCTYPE html>
<html lang="en">
    <head>
        <meta http-equiv="content-type" content="text/html; charset=UTF-8">
        <meta charset="utf-8">
        <title>Movie-UECE</title>
        <meta name="generator" content="Bootply" />
        <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
        <link href="css/bootstrap.min.css" rel="stylesheet">
        <!--[if lt IE 9]>
                <script src="//html5shim.googlecode.com/svn/trunk/html5.js"></script>
        <![endif]-->
        <link href="css/styles.css" rel="stylesheet">
    </head>
    <body>
        <!--login modal-->
        <div id="loginModal" class="modal show" tabindex="-1" role="dialog" aria-hidden="true">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <button type="button" class="close" data-dismiss="modal" aria-hidden="true">×</button>
                        <h1 class="text-center">Sistema Movie-UECE</h1>
                    </div>
                    <div class="modal-body">
                        <form method="post" action="validaLogin" class="form col-md-12 center-block">
                            <%
                                if(request.getAttribute("error") == "true"){
                            %>    
                            <div class="alert alert-danger" >
                                <strong>Login Inválido!</strong>
                            </div>
                            <%
                                }    
                            %>        
                            <div class="form-group">
                                <input type="text" class="form-control input-lg" name="username" placeholder="Usuario">
                            </div>
                            <div class="form-group">
                                <input type="password" class="form-control input-lg" name="password" placeholder="Senha">
                            </div>
                            <div class="form-group">
                                <input class="btn btn-primary btn-lg btn-block" type="submit" value="Acessar" />

                            </div>
                        </form>
                    </div>
                    <div class="modal-footer">
                        <div class="media">
                            <div class="media-middle">
                                <a href="#">
                                    
                                     <h4 class="media-heading" align="center">Frameworks</h4>
                                </a>
                            </div>
                            <center>
                               
                                <img class="media-object" src="images/uece.png" alt="...">
                            </center>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <!-- script references -->
        <script src="//ajax.googleapis.com/ajax/libs/jquery/2.0.2/jquery.min.js"></script>
        <script src="js/bootstrap.min.js"></script>
    </body>
</html>