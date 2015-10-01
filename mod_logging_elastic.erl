%% @author Marc Worrell <marc@worrell.nl>
%% @copyright 2015 Marc worrell
%% @doc Logs events, email-actions and messages to Elastic Search / Kibana

%% Copyright 2015 Marc Worrell
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% 
%%     http://www.apache.org/licenses/LICENSE-2.0
%% 
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(mod_logging_elastic).
-author("Marc Worrell <marc@worrell.nl>").
-behaviour(gen_server).

-mod_title("Logging to Elastic Search").
-mod_description("Logs events and debug/info/warning messages to Elastic Search / Kibana.").
-mod_prio(500).

%% gen_server exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/1]).
-export([
    pid_observe_zlog/3
]).

-include("zotonic.hrl").

-record(state, {
            site :: atom(),
            count :: integer(),
            mappings :: list(),
            batches
        }).

-define(BATCH_SIZE, 1000).
-define(COMMIT_CHECK_PERIOD, 1000).
-define(COMMIT_PERIOD, 60000).

-define(ELASTIC_URL, <<"http://127.0.0.1:9200/">>).

%% interface functions

pid_observe_zlog(Pid, #zlog{props=#log_email{}} = Msg, Context) ->
    gen_server:cast(Pid, set_defaults(Msg, Context));
pid_observe_zlog(Pid, #zlog{props=#log_message{}} = Msg, Context) ->
    gen_server:cast(Pid, set_defaults(Msg, Context));
pid_observe_zlog(Pid, #zlog{props=Props} = Msg, Context) when is_list(Props) ->
    gen_server:cast(Pid, set_defaults(Msg, Context));
pid_observe_zlog(_Pid, #zlog{} = Log, _Context) ->
    ?DEBUG({drop, Log}),
    undefined.


%%====================================================================
%% API
%%====================================================================
%% @spec start_link(Args) -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
start_link(Args) when is_list(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore               |
%%                     {stop, Reason}
%% @doc Initiates the server.
init(Args) ->
    {context, Context} = proplists:lookup(context, Args),
    Site = z_context:site(Context),
    timer:send_after(?COMMIT_CHECK_PERIOD, commit_check),
    timer:send_after(?COMMIT_PERIOD, commit),
    lager:md([
            {site, Site},
            {module, ?MODULE}
        ]),
    {ok, #state{
            site=Site,
            count=0,
            mappings=[],
            batches=dict:new()}}.

%% @spec handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% @doc Handling call messages
handle_call(Message, _From, State) ->
    {stop, {unknown_call, Message}, State}.

handle_cast(#zlog{} = Msg, State) ->
    State1 = queue_msg(Msg, State),
    {noreply, State1};

handle_cast(Message, State) ->
    {stop, {unknown_cast, Message}, State}.


%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
handle_info(commit_check, State) ->
    State1 = maybe_commit(State),
    timer:send_after(?COMMIT_CHECK_PERIOD, commit_check),
    {noreply, State1};
handle_info(commit, State) ->
    State1 = do_commit(State),
    timer:send_after(?COMMIT_PERIOD, commit_check),
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(_Reason, _State) ->
    ok.


%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%====================================================================
%% support functions
%%====================================================================

set_defaults(#zlog{timestamp=undefined} = Msg, Context) ->
    set_defaults(Msg#zlog{timestamp=os:timestamp()}, Context);
set_defaults(#zlog{user_id=undefined} = Msg, Context) ->
    Msg#zlog{user_id=z_acl:user(Context)};
set_defaults(Msg, _Context) ->
    Msg.

queue_msg(#zlog{} = Msg, State) ->
    Type = get_msg_type(Msg),
    Props = [ rename_field(KV) || KV <- binary_lists(add_log_props(Msg, get_msg_props(Msg))) ],
    State#state{batches=dict:append(Type, Props, State#state.batches), count=State#state.count+1}.

binary_lists(Ps) ->
    [ {K,case is_list(V) of true -> iolist_to_binary(V); false -> V end} || {K,V} <- Ps ].

%% Prevent Elastic Search runtime failures with its default FieldMapper mappings.
rename_field({mailer_host, V}) -> {mailer_hostname, V};    % Expects an integer
rename_field({envelop_to, V}) -> {envelop_recipient, V};   % Expects an integer
rename_field({envelop_from, V}) -> {envelop_sender, V};    % Expects an integer
rename_field(KV) -> KV.

maybe_commit(#state{count=Count} = State) when Count >= 1 ->
    do_commit(State);
maybe_commit(#state{} = State) ->
    State.

do_commit(#state{batches=Batches, site=Site} = State) ->
    List = dict:to_list(Batches),
    State1 = ensure_type_mappings(List, State),
    spawn_committer(List, Site, elastic_url(Site)),
    State1#state{batches=dict:new(), count=0}.

ensure_type_mappings([], State) ->
    State;
ensure_type_mappings(List, #state{mappings=Mappings, site=Site} = State) ->
    Keys = proplists:get_keys(List),
    Added = define_mappings(Keys -- Mappings, List, Site),
    State#state{mappings=Added++Mappings}.

elastic_url(Site) when is_atom(Site) ->
    Url = m_config:get_value(?MODULE, elastic_url, z_context:new(Site)),
    case z_utils:is_empty(Url) of
        true -> ?ELASTIC_URL;
        false -> Url
    end.

get_msg_type(#zlog{type=undefined, props=#log_email{}}) ->
    email;
get_msg_type(#zlog{type=undefined, props=#log_message{type=Type}}) ->
    Type;
get_msg_type(#zlog{type=Type}) ->
    Type.

get_msg_props(#zlog{props=#log_email{} = Email}) ->
    Ps = lists:zip(record_info(fields, log_email), tl(tuple_to_list(Email))),
    proplists:delete(props, Ps);
get_msg_props(#zlog{props=#log_message{} = Msg}) ->
    Ps = lists:zip(record_info(fields, log_message), tl(tuple_to_list(Msg))),
    proplists:delete(props, Ps);
get_msg_props(#zlog{props=Props}) when is_list(Props) ->
    Props.

add_log_props(#zlog{user_id=UserId, timestamp=Timestamp}, Props) ->
    [
        {'zlog_user_id', UserId},
        {'zlog_timestamp', z_dateformat:format(to_universal_time(Timestamp), "c", [])}
        | Props
    ].

to_universal_time({A,B,C} = Tm) when is_integer(A), is_integer(B), is_integer(C) ->
    calendar:now_to_universal_time(Tm);
to_universal_time(DT) ->
    DT.

spawn_committer([], _Site, _Url) ->
    ok;
spawn_committer(_Batches, Site, Empty) when Empty =:= undefined; Empty =:= <<>> ->
    lager:info("[~p] No elastic search url defined for ~p, dropping log messages",
               [Site, ?MODULE]);
spawn_committer(Batches, Site, Url) ->
    erlang:spawn_link(
            fun() ->
                commit_batches(Batches, Site, Url)
            end).

commit_batches([], _Site, _Url) ->
    ok;
commit_batches([{Key,List}|Bs], Site, Url) ->
    commit_batch(Key, List, Site, Url),
    commit_batches(Bs, Site, Url).

commit_batch(Type, List, Site, Url) ->
    Body = iolist_to_binary([ [ <<"{\"index\":{}}", 10>>, json(Props), 10 ] || Props <- List ]),
    Url1 = z_string:trim_right(z_convert:to_list(Url), $/)
            ++ "/"
            ++ "zotonic-log-" ++ z_utils:url_encode(z_convert:to_list(Site)) % Add the site as ES index
            ++ "/"
            ++ z_utils:url_encode(z_convert:to_list(Type))
            ++ "/_bulk",
    case httpc:request(post, 
                       {z_convert:to_list(Url1), httpc_headers(Site), "application/json", Body},
                       [],
                       httpc_options()) 
    of
        {ok, {{_Http, 200, _Ok}, _Hs, _Body}} ->
            lager:debug("[~p] Added ~p log events to Elastic Search index 'zotonic-log-~p'", [Site, Site]),
            ok;
        Other ->
            lager:error("[~p] Logging to elastic returned ~p", [Site, Other]),
            Other
    end.

define_mappings(Types, BatchList, Site) ->
    define_mappings_1(Types, BatchList, Site, elastic_url(Site), []).

define_mappings_1([], _BatchList, _Site, _Url, Added) ->
    Added;
define_mappings_1([Type|Types], BatchList, Site, Url, Added) ->
    JSON = mappings_from_values(hd(proplists:get_value(Type, BatchList)), []),
    Body = iolist_to_binary(mochijson2:encode({struct, [{Type, {struct, [{properties, JSON}]}}]})),
    Url1 = z_string:trim_right(z_convert:to_list(Url), $/)
            ++ "/"
            ++ "zotonic-log-" ++ z_utils:url_encode(z_convert:to_list(Site)) % Add the site as ES index
            ++ "/_mapping/"
            ++ z_utils:url_encode(z_convert:to_list(Type)),
    case httpc:request(post, 
                       {z_convert:to_list(Url1), httpc_headers(Site), "application/json", Body}, 
                       [],
                       httpc_options())
    of
        {ok, {{_Http, 200, _Ok}, _Hs, _Body}} ->
            lager:debug("[~p] Added mappings for ~p log events to Elastic Search index 'zotonic-log-~p'", [Site, Type, Site]),
            define_mappings_1(Types, BatchList, Site, Url, [Type|Added]);
        {ok, {{_Http, 404, _Ok}, _Hs, _Body}} ->
            lager:debug("[~p] Added mappings for ~p log events to Elastic Search index 'zotonic-log-~p'", [Site, Type, Site]),
            case ensure_index(Site, Url) of
                ok -> define_mappings_1([Type|Types], BatchList, Site, Url, [Type|Added]);
                _Other -> Added
            end;
        Other ->
            lager:error("[~p] Adding mappings for ~p to elastic returned ~p", [Site, Type, Other]),
            define_mappings_1(Types, BatchList, Site, Url, Added)
    end.

mappings_from_values([], Acc) ->
    lists:reverse(Acc);
mappings_from_values([{K,V}|Rest], Acc) ->
    case guess_type(z_convert:to_binary(K), V) of
        undefined ->
            mappings_from_values(Rest, Acc);
        Type ->
            Mapping = {K, {struct, [{type, Type}]}},
            mappings_from_values(Rest, [Mapping|Acc])
    end.

guess_type(<<"zlog_timestamp">>, _) ->
    <<"date">>;
guess_type(<<"timestamp">>, _) ->
    <<"date">>;
guess_type(<<"timestamp_", _/binary>>, _) ->
    <<"date">>;
guess_type(<<"date">>, _) ->
    <<"date">>;
guess_type(<<"date_", _/binary>>, _) ->
    <<"date">>;
guess_type(<<"created">>, _) ->
    <<"date">>;
guess_type(<<"modified">>, _) ->
    <<"date">>;
guess_type(<<"is_", _/binary>>, _) ->
    <<"boolean">>;
guess_type(_, V) when is_integer(V) ->
    <<"integer">>;
guess_type(_, {{Y,M,D},{H,I,S}}) 
    when is_integer(Y), is_integer(M), is_integer(D),
         is_integer(H), is_integer(I), is_integer(S) ->
    <<"date">>;
guess_type(_, V) when is_boolean(V) ->
    <<"boolean">>;
guess_type(_, _) ->
    undefined.


httpc_options() ->
    [
        {body_format, binary}
    ].

httpc_headers(Site) ->
    Context = z_context:new(Site),
    case {m_config:get_value(?MODULE, basic_auth_user, Context),
          m_config:get_value(?MODULE, basic_auth_pw, Context)}
    of
        {undefined,_} ->
            [];
        {_,undefined} ->
            [];
        {<<>>,<<>>} ->
            [];
        {User,Pass} ->
            [ {"Authorization", "Basic "++base64:encode_to_string(iolist_to_binary([User,$:,Pass]))} ]
    end.

ensure_index(Site, Url) ->
    Body = <<>>,
    Url1 = z_string:trim_right(z_convert:to_list(Url), $/)
            ++ "/"
            ++ "zotonic-log-" ++ z_utils:url_encode(z_convert:to_list(Site)), % Add the site as ES index
    case httpc:request(post,
                       {z_convert:to_list(Url1), httpc_headers(Site), "application/json", Body},
                       [],
                       httpc_options()) 
    of
        {ok, {{_Http, 200, _Ok}, _Hs, _Body}} ->
            lager:debug("[~p] Added index 'zotonic-~p' to Elastic Search", [Site, Site]),
            ok;
        {ok, {{_Http, 400, _Ok}, _Hs, Body}} = Other ->
            case binary:match(Body, <<"IndexAlreadyExistsException">>) of
                nomatch ->
                    lager:error("[~p] Adding index zotonic-log-~p to elastic returned ~p", [Site, Site, Other]),
                    Other;
                _ -> 
                    ok
            end;
        Other ->
            lager:error("[~p] Adding index zotonic-log-~p to elastic returned ~p", [Site, Site, Other]),
            Other
    end.


json(Props) ->
    iolist_to_binary(mochijson2:encode(z_convert:to_json(maybe_map_values(Props)))).

maybe_map_values(Props) ->
    [ maybe_map_value(P) || P <- Props ].

%% Kibana wants country codes in uppercase
maybe_map_value({address_country, V}) when is_binary(V) ->
    {address_country, z_string:to_upper(V)};
maybe_map_value({mail_country, V}) when is_binary(V) ->
    {mail_country, z_string:to_upper(V)};
maybe_map_value({country, V}) when is_binary(V) ->
    {country, z_string:to_upper(V)};
maybe_map_value({K, V} = P) when is_binary(V) ->
    case z_convert:to_binary(K) of
        <<"country_", _/binary>> -> {K, z_string:to_upper(V)};
        _ -> P
    end;
maybe_map_value(P) ->
    P.

