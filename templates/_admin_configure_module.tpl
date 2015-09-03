{% wire id=#form 
        type="submit" 
        postback={config_save module=`mod_logging_elastic`} 
        delegate=`mod_admin_config`
        on_success={dialog_close}
%}
<form id="{{ #form }}" action="postback">
    <div class="modal-body">
        <div class="form-group row">
            <label class="control-label">{_ Elastic Search URL _}</label>
            <input type="text" id="{{ #url }}" name="elastic_url" 
                   value="{{ m.config.mod_logging_elastic.elastic_url.value|escape }}" class="do_autofocus col-lg-4 col-md-4 form-control"
                   placeholder="http://127.0.0.1:9200/" />
        </div>
        <div class="form-group row">
                <p class="info-block">
                    {_ Specify the HTTP URL for posting log events to Elastic Search. _}
                </p>
            </div>
        </div>
        <div class="form-group row">
            <label class="control-label">{_ Username _}</label>
            <input type="text" id="{{ #url }}" name="basic_auth_user" 
                   value="{{ m.config.mod_logging_elastic.basic_auth_user.value|escape }}" class="col-lg-4 col-md-4 form-control" />
        </div>
        <div class="form-group row">
            <label class="control-label">{_ Password _}</label>
            <input type="text" id="{{ #url }}" name="basic_auth_pw" 
                   value="{{ m.config.mod_logging_elastic.basic_auth_pw.value|escape }}" class="col-lg-4 col-md-4 form-control" />
        </div>
    </div>

    <div class="modal-footer">
        {% button class="btn btn-default" text=_"Close" action={dialog_close} tag="a" %}
        <button class="btn btn-primary" type="submit">{_ Save _}</button>
    </div>
</form>
