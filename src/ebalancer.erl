%oioikoh
-module(ebalancer).
-compile(export_all).
-import(lists, [reverse/1]).


-define(NL, "\r\n").
% Разделитель между частями протокола Redis
-define(CRLF, "\r\n").
-define(SOCKET_OPTS, [binary, {active, false}, {packet, raw}, {reuseaddr, true}]).
% Время ожидания ответа от Redis сервера
-define(RECV_TIMEOUT, 5000).
% Обычно ожидаемая длина ответа от redis
-define(LENGTH_REPLY_REDIS, 1024).
-record(state, {
          host :: string() | undefined,
          port :: integer() | undefined,
          password :: binary() | undefined,
          database :: binary() | undefined,
%          reconnect_sleep :: reconnect_sleep() | undefined,

          socket :: port() | undefined
%          parser_state :: #pstate{} | undefined,
%          queue :: queue() | undefined
}).

%% @doc: Подключиться к Redis на основании параметров переданных в State
%% Эта функция синнхронная, если Redis вернет что - то, что мы не ожидаем, 
%% то мы просто падаем. Возвращается {ok, State} или {error,{SomeError, Reason}}.
%% где SomeError вид ошибки, Reason пояснения к ней
connect(State) ->
    case gen_tcp:connect(State#state.host, State#state.port, ?SOCKET_OPTS) of
        {ok, Socket} ->
            case authenticate(Socket, State#state.password) of
                ok ->
                    case select_database(Socket, State#state.database) of
                        ok ->
                            {ok, State#state{socket = Socket}};
                        {error, Reason} ->
                            {error, {select_error, Reason}}
                    end;
                {error, Reason} ->
                    {error, {authentication_error, Reason}}
            end;
        {error, Reason} ->
            {error, {connection_error, Reason}}
    end.

%% Настраиваемся на нужную базу в Redis
select_database(_Socket, undefined) ->
    ok;
select_database(Socket, Database) ->
    do_sync_command(Socket, ["SELECT", " ", Database, ?NL]).

authenticate(_Socket, <<>>) ->
    ok;
authenticate(_Socket, undefined ) ->
    ok;
authenticate(Socket, Password) ->
    do_sync_command(Socket, ["AUTH", " ", Password, ?NL]).

%% @doc: Исполняет команды синхронно ожидая от Redis
%% возврата "+OK\r\n", в противном случае все будет все плохо.
do_sync_command(Socket, Command) ->
    inet:setopts(Socket, [{active, false}]),
    case gen_tcp:send(Socket, Command) of
        ok ->
            %% Hope there's nothing else coming down on the socket..
            case gen_tcp:recv(Socket, 0, ?RECV_TIMEOUT) of
                {ok, <<"+OK\r\n">>} ->
                    inet:setopts(Socket, [{active, once}]),
                    ok;
                Other ->
                    {error, {unexpected_data, Other}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

% текущее состояние получения ответа от Redis, фактически это сокращение от state reply
-record( state_r, {type   = undefined ::atom(),    %% тип данных полученных от Redis
                   wait_r = true      ::boolean(), %% ожидаем прихода последнего символа \r
                   wait_n = true      ::boolean()  %% ожидаем прихода последнего симовла \n
                      }).

%%
%% Поcлать произвольную команду Cmd в конкретный узел Redis по сокету Socket  и получить ответ от него
%%
put_cmd_to_node( Cmd, Socket )->
    inet:setopts(Socket, [{active, false}] ),
    case  gen_tcp:send(Socket, Cmd ) of  
       ok ->
            receive_reply_from_node(Socket, #state_r{}, []);
       {error, Reason} ->
            {error, Reason}
    end.


% Еще не известно, что за данные пришли от Redis
get_state_r( StateR, Bin ) 
  when StateR#state_r.type == undefined ->
    NewStateR = StateR#state_r{ type = type_redis_data( Bin ) },
    get_state_r( NewStateR, Bin );
% От Redis пришла строка или ошибка c окончанием передачи
get_state_r( StateR, << _, $\r, $\n >> )
  when  StateR#state_r.type == r_string,
        StateR#state_r.type == r_error  ->
     StateR#state_r{ wait_r = false, wait_n = false  };
% От Redis пришел символ \r, но не пришел \n
get_state_r( StateR, << _, $\n >> ) 
  when  StateR#state_r.type == r_string,
        StateR#state_r.type == r_error ->
    StateR#state_r{ wait_r = false, wait_n = true  };
% От Redis не пришло \r
get_state_r( StateR, _Bin ) 
  when StateR#state_r.type == r_string,
       StateR#state_r.type == r_error ->
    StateR.
 
% Получить тип данных поступивших в ответе от Redis
type_redis_data( Bin ) ->
  case  binary:first(Bin) of 
      43 -> r_string; % +
      42 -> r_array;  % *
      58 -> r_integer;% :
      45 -> r_error;  % -
      36 -> r_bulk    % $
  end.

   
% Передача данных от Redis окончена
receive_reply_from_node( _, StateR,  SoFar ) 
  when StateR ==  #state_r{wait_r = false, wait_n = false} ->
    list_to_binary( lists:reverse(SoFar) );
% Продолжается передача данных от Redis
receive_reply_from_node(Socket, StateR, SoFar) ->
   case gen_tcp:recv(Socket, 0, ?RECV_TIMEOUT) of
       {ok, Bin } ->
            io:format("Reply Redis ( ~p~n )",[Bin] ),
            % Здесь надо разбирать ответ
%            io:format("Reply Redys ( ~p~n )",[binary_to_term(Bin)] ),
            receive_reply_from_node(Socket, get_state_r( StateR, Bin), [Bin|SoFar] );
       {error,closed} ->
            io:format("Reply Redis socket closed"),
            io:format("Reply Redis socket closed ( ~p~n )",[SoFar] ),
            list_to_binary(lists:reverse( [SoFar]));
       {error, Reason } ->
            {error,Reason};
       _ ->
        io:format("undefined redis reply")
   end.


 



%init( Host, Port, Database ) ->
init(  ) ->
   Host = "localhost", Port = 6379, Database = "0",
   InitState = #state{ host = Host, port = Port, database = Database },
   case  connect( InitState ) of
     {ok,State}->
	   start_server(State);
     {error,{_SomeError, Reason}} ->
           io:format("Connection to Redis has error! ( ~p~n )",[Reason] )
   end.
    
%не забыть отключиться от Redis по окончании работы

%%  
%%  Запускаем наш сервер в который полетят команды
%%
start_server( State ) ->
    {ok, Listen} = gen_tcp:listen(2345, [binary, {packet, 4},
					 {reuseaddr, true},
					 {active, true}]),
    spawn( fun()-> wait_incoming_connect( Listen, State )   end ).

% Обработка входящего соединения
wait_incoming_connect( Listen, State ) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    spawn( fun() -> wait_incoming_connect( Listen, State ) end ),
    loop( Socket, State ).

loop(Socket,State) ->
    receive
	{tcp, Socket, Bin} ->
            % Сервер получил бинарные данные 
	    io:format("Server received binary = ~p~n",[Bin]),
	    Str = binary_to_term(Bin), 
	    io:format("Server (unpacked)  ~p~n",[Str]),
%	    Reply = Str,
            % Пересылаем команду а Redis
            case put_cmd_to_node( Str, State#state.socket ) of
                {error,Reason} ->
	             io:format("Server replying error ~p~n", [Reason]),
                     ok = gen_tcp:send(Socket, term_to_binary(Reason) );
                ReplyRedis ->
                     Reply =  binary_to_term(ReplyRedis), 
	             io:format("Server replying = ~p~n",[Reply]),
	             ok = gen_tcp:send(Socket, ReplyRedis )
            end, 
%	    gen_tcp:send(Socket, term_to_binary(Reply)),  
	    loop(Socket,State);
	{tcp_closed, Socket} ->
	    io:format("Server socket closed~n")
    end.


