<%-- 
    Document   : devolucao
    Created on : 30/07/2015, 01:25:26
    Author     : diegogomestome
--%>
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
        <input type="hidden" name="logica" value="DevolverAluguel">
        <fieldset>
            <legend>Devolver</legend>

            <div class="form-group">
                <label for="inputPassword" class="col-lg-2 control-label">CPF Cliente</label>
                <div class="col-lg-10">


                    <div class="col-lg-10">
                        <input type="text" name="cpfCliente" class="form-control" id="inputPassword" placeholder="CPF">                    
                    </div>
                </div>
            </div>

        

            <div class="form-group" >
                <label for="disabledTextInput" class="col-sm-2 control-label" >Data Devolução</label>
                <div class="col-lg-10">
                    <div class="col-lg-10">
                        <input type="text"  class="data form-control " name="data">
                    </div>
                </div>
            </div>

            <div class="form-group">
                <div class="col-lg-10 ">
                    <div class="col-lg-12 col-lg-offset-9">

                        <button type="submit" class="btn btn-info">Efetivar Devolução</button>
                    </div>
                </div>
            </div>
        </fieldset>
    </form>
</div>
