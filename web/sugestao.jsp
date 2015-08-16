<%-- 
    Document   : sugestao
    Created on : 30/07/2015, 01:25:41
    Author     : diegogomestome
--%>

<div class="well bs-component">
   
        <input type="hidden" name="logica" value="AdicionaAluguel">
        <fieldset>
            <legend>Sugestão de filmes</legend>

         

            <div id="film">
                <div class="form-group" >
                    <form class="navbar-form  container"  role="search" action="filmeMvc">
                        <input type="hidden" name="logica" value="SugerirFilmes"/>
                        <div class="form-group">
                            <input type="text" name="codigo" class="form-control" placeholder="Código do Filme">
                        </div>
                        <button type="submit" class="btn btn-default">Buscar</button>

                    </form>
                </div> 
            </div>

                
        </fieldset>
    
</div>
