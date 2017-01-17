-module(industry_seestar_helper_test).

-include_lib("eunit/include/eunit.hrl").

prepare_select_test() ->
    Keyspace = "keyspace",
    Attributes = [{id, string},
        {value, string}],
    Schema = [{name, Keyspace}, {type, table}, {attributes, Attributes}],
    "SELECT id,value FROM keyspace.table WHERE id='id_value'"
        = industry_seestar_helper:prepare_select(Keyspace, table, Schema, <<"id_value">>).

prepare_secondary_index_select_test() ->
    Keyspace = "keyspace",
    Attributes = [{id, string},
        {secondary_index, string},
        {value, string}],
    Schema = [{name, Keyspace}, {type, table}, {attributes, Attributes}],
    "SELECT id,secondary_index,value FROM keyspace.table WHERE secondary_index='secondary_index_key'"
        = industry_seestar_helper:prepare_select(Keyspace, table, Schema, [{secondary_index, <<"secondary_index_key">>}]).

prepare_multiple_select_test() ->
    Keyspace = "keyspace",
    Attributes = [{id, string},
        {secondary_index, string},
        {secondary_index2, string},
        {value, string}],
    Schema = [{name, Keyspace}, {type, table}, {attributes, Attributes}],
    "SELECT id,secondary_index,secondary_index2,value FROM keyspace.table WHERE secondary_index='secondary_index_key' AND secondary_index2='secondary_index2'"
        = industry_seestar_helper:prepare_select(Keyspace, table, Schema, [{secondary_index, <<"secondary_index_key">>}, {secondary_index2, <<"secondary_index2">>}]).

create_table_compound_primary_key_test() ->
    Keyspace = "keyspace",
    Env = table,
    Attributes = [{id, string},
        {primary_key, string},
        {key, string},
        {value, string}],
    Schema = [{name, Keyspace}, {type, Env}, {attributes, Attributes}],
    "CREATE TABLE keyspace.table( id varchar primary_key varchar key varchar value varchar, PRIMARY KEY ( id , primary_key ))" =
        industry_seestar_helper:prepare_create_table(Keyspace, Schema, table, [id, primary_key]).

create_table_id_primary_key_test() ->
    Keyspace = "keyspace",
    Type = table,
    Attributes = [{id, string},
        {key, string},
        {value, string}],
    Schema = [{name, Keyspace}, {type, Type}, {attributes, Attributes}],
    "CREATE TABLE keyspace.table( id varchar PRIMARY KEY , key varchar , value varchar )" =
        industry_seestar_helper:prepare_create_table(Keyspace, Schema, Type).