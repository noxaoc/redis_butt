%хрень
-module(eb_client).
-compile(export_all).

% Послать команду Redis
send_to_redis(Str) ->
    {ok, Socket} = gen_tcp:connect("localhost", 2345,
		        	[binary, {packet, 4}]),
    ok = gen_tcp:send(Socket, term_to_binary(Str)),
    wait_server_repl(Socket,[]),
    gen_tcp:close(Socket).
   
wait_server_repl(Socket,SoFar)->
    receive
	{tcp,Socket,Bin} ->
            wait_server_repl( Socket, [Bin | SoFar ] );
        {tcp_closed,Socket} ->
           Tmp = list_to_binary( lists:reverse( SoFar ) ),
           Val = binary_to_term(Tmp),
	    io:format("Client result = ~p~n",[Val]);
         _ -> 
	    io:format("Server reply garbage!")
    end.
    
