%%%-------------------------------------------------------------------
%%% @author Martin Kristiansen <msk@ajour.io>
%%% @copyright (C) 2015, Martin Kristiansen
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(industry_seestar_helper).

-export([prepare_insert/4,
	prepare_select/3,
	prepare_select/4,
	prepare_update/5,
	prepare_delete/4,
	prepare_create_table/3, prepare_create_table/5,
	prepare_create_tables/2,
	prepare_secondary_index/3]).


-export([format_row_results/2]).


prepare_insert(NameSpace, Table, Schema, Values) ->
    Attributes = i_utils:get(attributes, Schema),
    Query = [
	     io_lib:format("INSERT INTO ~s.~p", [NameSpace, Table]),
	     " (", string:join([ io_lib:format("~p", [K]) 
				 || {K,_} <- Attributes], ","), ") VALUES",
	     " (", string:join(["?" || _X <- Attributes], ","), ")"
	    ],
    Row = [ begin
		Value = proplists:get_value(Attribute, Values),
		i_utils:render_prepared(Value, AttrType)
	    end || {Attribute, AttrType} <- Attributes],
    {lists:flatten(Query), Row}.

-spec prepare_select(iolist(), atom(), [term()]) -> string().
prepare_select(NameSpace, Table, Schema) ->
	Attributes = i_utils:get(attributes, Schema),
	Query = [
		"SELECT ", string:join([io_lib:format("~p", [K])
			|| {K, _} <- Attributes], ","),
		" FROM ", io_lib:format("~s.~p", [NameSpace, Table])
	],
	lists:flatten(Query).

-spec prepare_select(iolist(), atom(), [term()], map() | iolist()) -> string().
prepare_select(NameSpace, Table, Schema, WhereMap) when is_map(WhereMap) ->
	WherePL = maps:to_list(WhereMap),
	Attributes = i_utils:get(attributes, Schema),
	Query = [
		"SELECT ", string:join([io_lib:format("~p", [K])
			|| {K, _} <- Attributes], ","),
		" FROM ", io_lib:format("~s.~p", [NameSpace, Table]),
		" WHERE ", string:join([io_lib:format("~s=~s", [K, i_utils:render(V, i_utils:get([attributes, K], Schema))]) || {K, V} <- WherePL], " AND ")
	],
	lists:flatten(Query);
prepare_select(NameSpace, Table, Schema, Id) ->
	Attributes = i_utils:get(attributes, Schema),
	Query = [
		"SELECT ", string:join([io_lib:format("~p", [K])
			|| {K, _} <- Attributes], ","),
		" FROM ", io_lib:format("~s.~p", [NameSpace, Table]),
		" WHERE id=", i_utils:render(Id, i_utils:get([attributes, id], Schema))
	],
	lists:flatten(Query).
    
prepare_update(NameSpace, Table, Schema, Id, Values) ->
    QueryAssignments = [begin
			    lager:debug("RENDERING ~p with Type of ~p ", [Key, Value]),
			    KeyType = i_utils:get([attributes, Key], Schema),
			    RenderedKey = i_utils:render(Value, KeyType),
			    io_lib:format("~p = ~s", [Key, RenderedKey])
			end || {Key, Value} <- Values],
    IdType = i_utils:get([attributes, id], Schema), 
    Query = [
	     io_lib:format("UPDATE ~s.~p", [NameSpace, Table]),
	     " SET ", string:join(QueryAssignments, ", "),
	     io_lib:format(" WHERE id= ~s", [i_utils:render(Id, IdType)]) 
	    ],
    lists:flatten(Query).

-spec prepare_delete(iolist(), atom(), [term()], iolist() | map()) -> string().
prepare_delete(NameSpace, Table, Schema, WhereMap) when is_map(WhereMap) ->
	WherePL = maps:to_list(WhereMap),
	Attributes = i_utils:get(attributes, Schema),
	Query = [
		"DELETE FROM ", io_lib:format("~s.~p", [NameSpace, Table]),
		" WHERE ", string:join([ io_lib:format("~s=~s",
			[K, i_utils:render(V, i_utils:get([attributes, K], Schema))]) || {K,V} <- WherePL], " AND ")
	],
	lists:flatten(Query);
prepare_delete(NameSpace, Table, Schema, Id) -> 
    Attributes = i_utils:get(attributes, Schema),
    Query = [
	     "DELETE FROM ", io_lib:format("~s.~p", [NameSpace, Table]),
	     " WHERE id=", i_utils:render(Id, i_utils:get([attributes, id], Schema))
	    ],
    lists:flatten(Query).


prepare_create_tables(NameSpace, Schemas) -> 
    Env = [{Type, Schema} || Schema <- Schemas, begin
						    Type = i_utils:get(type, Schema),
						    true
						end],
    lists:map(fun(Schema) ->
		      prepare_create_table(NameSpace, Schema, Env)
	      end, Schemas).
    
prepare_create_table(NameSpace, Schema, Env) ->
    Attributes = i_utils:get(attributes, Schema),
    Table      = i_utils:get(type,      Schema),
    Query = [
	     io_lib:format("CREATE TABLE ~s.~p", [NameSpace, Table]),
	     "(", string:join([begin
				   RType = i_utils:render_type(Type, Env),
				   Primary = case Key of
						 id -> "PRIMARY KEY ";
						 _ -> ""
					     end,
				   io_lib:format(" ~p ~s ~s", [Key, RType, Primary])
			       end || {Key, Type} <- Attributes],","),
	     ")"],
    lists:flatten(Query).

-spec prepare_create_table(string(), [term()], atom(), [atom()], [atom()]) -> string().
prepare_create_table(NameSpace, Schema, Env, Partition_Keys, Clustering_Keys) ->
	Attributes = i_utils:get(attributes, Schema),
	Table = i_utils:get(type, Schema),
	PK_Strings = [atom_to_list(PK) || PK <- Partition_Keys, proplists:is_defined(PK, Attributes)],
	CK_Strings = [atom_to_list(CK) || CK <- Clustering_Keys, proplists:is_defined(CK, Attributes)],
	Printed_Keys = render_primary_key({PK_Strings, CK_Strings}),
	Printed_Attr = string:join([begin
																RType = i_utils:render_type(Type, Env),
																io_lib:format(" ~p ~s", [Key, RType])
															end || {Key, Type} <- Attributes], ","),
	Query = [
		io_lib:format("CREATE TABLE ~s.~p", [NameSpace, Table]),
		"(", Printed_Attr, ",", Printed_Keys, ")"],
	lists:flatten(Query).

-spec prepare_secondary_index(iolist(), [term()], atom()) -> string().
prepare_secondary_index(NameSpace, Schema, Attribute) ->
	Attributes = i_utils:get(attributes, Schema),
	Table      = i_utils:get(type, Schema),
	true       = proplists:is_defined(Attribute, Attributes),
	Query = [
		io_lib:format("CREATE INDEX ON ~s.~p ( ~p )",
			[NameSpace, Table, Attribute])
	],
	lists:flatten(Query).

format_row_results(Row, Schema) ->
    Attributes = i_utils:get(attributes, Schema),
    [begin
	 Type = i_utils:get(Name, Attributes),
	 {Name, format_element(Value, Type)}
     end || {Name, Value} <- Row].


format_element(null,  integer) -> 
    undefined;
format_element(Value, integer) ->
    Value;
format_element(null,  boolean) -> 
    undefined;
format_element(Boolean,  boolean) ->
    (Boolean);
format_element(null,  string) -> 
    undefined;
format_element(Value,  string) ->
    binary_to_list(Value);
format_element(null, {set, Of}) ->
    sets:new();
format_element(Value, {enum, Of}) ->
    list_to_existing_atom(binary_to_list(Value));
format_element(Values, {set, Of}) ->
    sets:from_list(lists:map(fun(Value) ->
		      format_element(Value, Of)
	      end, sets:to_list(Values)));
format_element(null,  {list, Of}) ->
    [].

%% From tt_seestar in xmpp
-spec render_primary_key({list(), list()}) -> iolist().
render_primary_key({[],[]}) ->
	"";
render_primary_key({PartitionKeys, []}) ->
	PartitionKeyRendering = case PartitionKeys of
														[P] -> P;
														_ -> "( " ++ string:join(PartitionKeys, ", ") ++ " )"
													end,
	["PRIMARY KEY (", PartitionKeyRendering, ")"];
render_primary_key({PartitionKeys, ClusteringKeys}) ->
	PartitionKeyRendering = case PartitionKeys of
														[P] -> P;
														_ -> "( " ++ string:join(PartitionKeys, ", ") ++ " )"
													end,
	ClusteringRendering = case ClusteringKeys of
													[] -> [];
													_ -> string:join(ClusteringKeys, ", ")
												end,
	["PRIMARY KEY (", PartitionKeyRendering, ", ", ClusteringRendering, ")"].