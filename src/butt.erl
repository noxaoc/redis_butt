% butt ( грядка ) 
-module(butt).
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
% для каждого типа данных возвращаемых Redis сущетсвует число повторов CRLF после получения которых
% можно считать что данные получены полностью, например
% для  simple string, error, integer значение равно 1
% для bulk string равно 2, для массивов это число зависит от числа элементов в массиве и типов
% этих элементов
-record( state_r, {type   = undefined ::atom(),    %% тип данных полученных от Redis
                   wait_r = true      ::boolean(), %% ожидаем прихода последнего символа \r
                   wait_n = true      ::boolean(), %% ожидаем прихода последнего симовла \n
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

   
% Найти байт  возвращает позицию если нашли иначе nomatch
find_byte( Bin, Offset, Byte ) ->
   case  binary:match( Bin, [Byte],[ {scope, {Offset,1} } ] ) of
        {scope, {NewOffset,1}} ->
	   NewOffset;
         _ ->
          nomatch
   end.

% Получить данные для одного типа 


% Socket - сокет через который читаются данные из Redis
% Type - тип элемента читаемый из Bin по смещению
% CR - true пришел символ \r
% LR - true пришел символ \n
% Offset - смещение от начала при котором мы должны читать данные из Bin
read( Socket, Type, CR, LR, Bin, Offset ) 
 
read( Socket, Type, Bin, Offset ) 
  when Type == r_string; Type == r_error; Type == r_integer -> 
    read_simple( Socket, Type, Bin, Offset );
read( Socket, r_bulk, Bin, Offset ) -> 
    read_bulk( Socket, Bin, Offset );
read( Socket, r_array,  Bin, Offset ) -> 
    read_array( Socket,  Bin, Offset );
read() -> 


% Прочитать array до конца
% Возвращает { ok | continue, Bin, Offset }, где
% Bin - последний пакет данных, принятый от Redis
% Offset - смещение от начала пакета данных, начиная с которого идут непрочитанные данные
%            ok - пакет был полностью разобран и прочитан
%      continue - пакет еще можно разбирать и читать, начиная с Offset
read_array( Socket, Bin, Offset )->
    { Size, NewBin, NewOffset } = read_size_bulk( Socket, Bin, Offset ),
    read_array_elem( Socket, NewBin, NewOffset, Size ).

% Прочитать один элемент c номером Size массива до конца
% Возвращает { ok | continue, Bin, Offset }, где
% Bin - последний пакет данных, принятый от Redis
% Offset - смещение от начала пакета данных, начиная с которого идут непрочитанные данные
%            ok - пакет был полностью прочитан, больше из него нечего читать
%      continue - пакет еще можно читать, начиная с Offset
read_array_elem( _Socket, Bin, Offset, 0 ) 
  when size(Bin) >= Offset ->
    { ok, Bin, Offset }; 
read_array_elem( _Socket, Bin, Offset, 0 ) ->
  { continue, Bin, Offset }; 
read_array_elem( Socket, Bin, Offset, Size ) 
  when size(Bin) >= Offset -> % Запредельное смещение, по сути пакет вычитан до конца
    NewBin = get_new_packet( Socket ),
    read_array_elem( Socket, Bin, 0, Size );
read_array_elem( Socket, Bin, Offset, Size )->
  case binary:at( Bin, Offset ) of % читаем тип элемента 
      $$ ->
         read_bulk( Socket, NewBin, NewOffset );
      $* ->
         read_array( Socket, NewBin, NewOffset );
      _ when $+; $-; $: ->
         read_simple( Socket, NewBin, NewOffset )
  end,
  read_array_elem( Socket, NewBin, NewOffset, Size - 1 ).
 
% Прочитать bulk строку до конца
% Возвращает { ok | continue, Bin, Offset }, где
% Bin - последний пакет данных, принятый от Redis
% Offset - смещение от начала пакета данных, начиная с которого идут непрочитанные данные
%            ok - пакет был полностью прочитан
%      continue - пакет еще можно читать, начиная с Offset
read_bulk( Socket, Bin, Offset )->
    { Size, NewBin, NewOffset } = read_size_bulk( Socket, Bin, Offset ),
    read_bulk_string( Socket, Size + size(<<?CRLN>>), NewBin, NewOffset ).

% Прочитать размер  bulk строки
read_size_bulk( Socket, Bin, Offset )->
   { SizeBin, NewBin, NewOffset } = read_size_bulk_cr( Socket,<<>>, Bin, Offset ),
   Size = list_to_integer( binary_to_list(SizeBin) ),   
   <<Value:_IntSize/binary>>,
   { Value, NewBin, NewOffset }.
 
% Прочитать размер до символа CR
% Возвращает: { Size, Bin, Offset }, где
% Size - размер строки в текстовом бинарном виде
% Bin - последний пакет данных, принятый от Redis
% Offset - смещение от начала пакета данных, начиная с которого идут непрочитанные данные
read_size_bulk_cr( Socket, SizeBin, Bin, Offset )->
    case find_byte( Bin, Offset, ?CR ) of
         nomatch ->
               NewBin = get_new_packet( Socket ),
               read_size_bulk_cr( Socket, NewBin, 0 );
         NewOffset ->
               Data = binaty:part( Bin, Offset, NewOffset - Offset + 1 ),
               { _, NewBin, NewOffset2 } = read_size_bulk_ln( Socket, Bin, NewOffset + 1 ),
               { <<  SizeBin/binary, Data/binary >>, NewBin, NewOffset2 }
    end.
% Прочитать до символа LF
read_size_bulk_ln( Socket, Bin, Offset )->
    case find_byte( Bin, Offset, ?LF ) of
         nomatch ->
               NewBin =  get_new_packet( Socket ),
               read_size_bulk_ln( Socket, NewBin, 0 );
         NewOffset ->
               if
                  size(Bin) == ( NewOffset + 1 ) ->    
                     { ok, Bin, NewOffset + 1 };   % все прочитали до конца что хотели
                  true ->
                     { continue, Bin, NewOffset + 1 } % еще осталось что читать
               end
    end.

% Прочитать строку размером Size с учетом символов CRLN
read_bulk_string( Socket, Size,  Bin, NewOffset ) ->
    RestSize =  size(Bin) - NewOffset,   % Размер участка данных который еще можно дочитывать
    if
       RestSize <= 0 ->
             %Все вычитано до конца, отправляемся на чтение новых данных    
             NewBin = get_new_packet( Socket ),
             read_bulk_string( Socket, Size, NewBin, 0 ); 
       Size == RestSize ->
	     % Сейчас вычитаем строку до конца вместе с окончанием строки
             { ok, Bin, RestSize + NewOffset }; 
       Size < RestSize ->
             { continue, Bin, Size + NewOffset - 1  }; 
       true -> % Данных явно не достаточно
             NewBin =  get_new_packet( Socket ),
             read_bulk_string( Socket, Size - RestSize, NewBin, 0 ) 
    end.
    


% Прочитать простой тип
% Возвращает { ok | continue, Bin, Offset }, где
% Bin - последний пакет данных принятый от Redis
% Offset - смещение от начала пакета данных, начиная с которого идут непрочитанные данные
% ok - пакет был полностью прочитан, continue - пакет еще можно читать начиная с Offset
read_simple( Socket, Bin, Offset )->
    read_simple_cr( Socket, Bin, Offset ).

read_simple_cr( Socket, Bin, Offset )->
    case find_byte( Bin, Offset, ?CR ) of
         nomatch ->
               NewBin = get_new_packet( Socket ),
               read_simple_cr( Socket, NewBin, 0 );
         NewOffset ->
               read_simple_ln( Socket, Bin, NewOffset + 1 )
    end.

read_simple_ln( Socket, Bin, Offset )-> 
     case find_byte( Bin, Offset, ?LF ) of
         nomatch ->
               NewBin = get_new_packet( Socket ),
               read_simple_ln( Socket, NewBin, 0 );
         NewOffset ->
               if
                  size(Bin) == ( NewOffset + 1 ) ->    
                     { ok, Bin, NewOffset + 1 };   % все прочитали что хотели
                  true ->
                     { continue, Bin, NewOffset + 1 } % еще осталось что читать
               end
     end.




% Получить новый пакет от Redis
get_new_packet( Socket ) ->
    gen_tcp:recv( Socket, 0, ?RECV_TIMEOUT ).


 



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


