<%-- 
    Document   : adicionaUsuario
    Created on : 26/07/2015, 10:24:57
    Author     : diegogomestome
--%>

<script>
    $(document).ready(function () {
        var count = 1;
        $("#btn1").click(function () {
            count++;
            if (count < 4)
                $("#film").append(" <div class=\"form-group\"> <label for=\"inputPreco\" class=\"col-lg-2 control-label\"></label> <div class=\"col-lg-10\"><div class=\"col-lg-10\"><input type=\"text\" name=\"filme" + count + "\"class=\"form-control\"  placeholder=\"Filme\"></div> </div> </div>");
        });
    });

</script>
<script type="text/javascript">
    $(document).ready(function () {
        $(".data").datepicker({
            dateFormat: 'dd/mm/yy',
            dayNames: ['Domingo', 'Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo'],
            dayNamesMin: ['D', 'S', 'T', 'Q', 'Q', 'S', 'S', 'D'],
            dayNamesShort: ['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom'],
            monthNames: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'],
            monthNamesShort: ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'],
            nextText: 'Próximo',
            prevText: 'Anterior'
        });
    });
</script>




<div class="well bs-component">
    <form class="form-horizontal" action="aluguelMvc" method="POST">
        <input type="hidden" name="logica" value="AdicionaAluguel">
        <fieldset>
            <legend>Novo Aluguel</legend>

            <div class="form-group">
                <label for="inputPassword" class="col-lg-2 control-label">CPF Cliente</label>
                <div class="col-lg-10">


                    <div class="col-lg-10">
                        <input type="text" name="cpfCliente" class="form-control" id="inputPassword" placeholder="CPF">                    
                    </div>
                </div>
            </div>

            <div id="film">
                <div class="form-group" >
                    <label for="inputPassword" class="col-lg-2 control-label">Codigo Filme</label>
                    <div class="col-lg-10">


                        <div class="col-lg-10">
                            <input type="text" name="filme1" class="form-control" id="inputPassword" placeholder="Filme">                    
                        </div>

                    </div>
                    <div class="col-lg-10 ">
                        <div class="col-lg-9 col-lg-offset-7">
                            <button type="button" class="btn-info"id="btn1">Adicionar Filme</button>
                        </div>
                    </div>
                </div> 
            </div>

            <div class="form-group" >
                <label for="disabledTextInput" class="col-sm-2 control-label" >Data Aluguel</label>
                <div class="col-lg-10">
                    <div class="col-lg-10">
                        <input type="text"  class="data form-control " name="data">
                    </div>
                </div>
            </div>

            <div class="form-group">
                <div class="col-lg-10 ">
                    <div class="col-lg-12 col-lg-offset-9">

                        <button type="submit" class="btn btn-info">Efetivar Aluguel</button>
                    </div>
                </div>
            </div>
        </fieldset>
    </form>
</div>
