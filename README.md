ErlasticSearch
=========
A thrift based erlang client for [ElasticSearch](http://www.elasticsearch.org/).



Installation
============
Add this as a rebar dependency to your project.

1. Be sure to set up ElasticSearch to support thrift!
   * You'll need to add (at least) the following settings to config.yaml
      * ```thrift.port: 9500```
      * ```thrift.protocol: 'binary'```
    * You might want to set the port to whatever you want instead of ```9500```
1. Update your environment with the following parameters (look in [app.config](https://github.com/dieswaytoofast/erlasticsearch/blob/master/app.config) for examples)
   * ```thrift.options```
   * ```thrift.host```
   * ```thrift.port```
1. Profit


Details
============
1. Any JSON expected by ElasticSearch will need to go in as JSON  
   * For example --> ```<<"{\"settings\":{\"number_of_shards\":3}}">>```)
1. Output returned by most everything is in the form ```{ok, #restResponse{}} | error()```
   * See the format of #restResponse{} [here](https://github.com/dieswaytoofast/erlasticsearch/blob/master/src/elasticsearch_types.hrl).
   * See the format of error() [here](https://github.com/dieswaytoofast/erlasticsearch/blob/master/src/erlasticsearch.hrl)
   * The payload from ElasticSearch - when it exists - will almost always be JSON
      * e.g. --> ```<<"{\"ok\":true,\"acknowledged\":true}">>```
1. Boolean methods (e.g. ```is_index/1```) return a ```boolean()``` (d-uh)




Index CRUD
-----
These methods are available to perform CRUD activities on Indexes (kinda, sorta, vaguely the equivalent of Databases)

Function | Parameters | Description
----- | ----------- | --------
create_index/1 | IndexName  | Creates the Index called _IndexName_
create_index/2 | IndexName, Parameters | Creates the Index called _IndexName_, with additional options as specified [here](http://www.elasticsearch.org/guide/reference/api/admin-indices-create-index/)
delete_index/1 | IndexName  | Deletes the Index called _IndexName_
is_index/1 | IndexName  | Checks if the Index called _IndexName_ exists



**EXAMPLES**

```erlang
erlasticsearch@pecorino)1> erlasticsearch:create_index(<<"foo2">>).
{ok,{restResponse,200,undefined,
                  <<"{\"ok\":true,\"acknowledged\":true}">>}}
erlasticsearch@pecorino)2> erlasticsearch:create_index(<<"foo3">>, <<"{\"settings\":{\"number_of_shards\":3}}">>).
{ok,{restResponse,200,undefined,
                  <<"{\"ok\":true,\"acknowledged\":true}">>}}
```
```erlang
erlasticsearch@pecorino)3> erlasticsearch:delete_index(<<"foo2">>).                                               
{ok,{restResponse,200,undefined,
                  <<"{\"ok\":true,\"acknowledged\":true}">>}}
```
```erlang
erlasticsearch@pecorino)4> erlasticsearch:is_index(<<"foo2">>).    
false
erlasticsearch@pecorino)5> erlasticsearch:is_index(<<"foo3">>).
true
```



Document CRUD
-----
These methods are available to perform CRUD activities on actual documents 

Function | Parameters | Description
----- | ----------- | --------
insert_doc/4 | IndexName, Type, Id, Doc  | Creates the Doc under _IndexName_, with type _Type_, and id _Id_
insert_doc/5 | IndexName, Type, Id, Doc, Params  | Creates the Doc under _IndexName_, with type _Type_, and id _Id_, and passes the tuple-list _Params_ to ElasticSearch
get_doc/3 | IndexName, Type, Id  | Gets the Doc under _IndexName_, with type _Type_, and id _Id_
get_doc/4 | IndexName, Type, Id, Params  | Gets the Doc under _IndexName_, with type _Type_, and id _Id_, and passes the tuple-list _Params_ to ElasticSearch
delete_doc/3 | IndexName, Type, Id  | Deleset the Doc under _IndexName_, with type _Type_, and id _Id_
delete_doc/4 | IndexName, Type, Id, Params  | Deletes the Doc under _IndexName_, with type _Type_, and id _Id_, and passes the tuple-list _Params_ to ElasticSearch

_Note_: For both ```insert_doc/4``` and ```insert_doc/5```, sending in ```undefined``` as the ```Id``` will result in ElasticSearch generating an Id for the document.  This Id will be returned as part of the result...


**EXAMPLES**

```erlang
erlasticsearch@pecorino)6> erlasticsearch:insert_doc(<<"index1">>, <<"type1">>, <<"id1">>, <<"{\"some_key\":\"some_val\"}">>).
{ok,{restResponse,201,undefined,
                  <<"{\"ok\":true,\"_index\":\"index1\",\"_type\":\"type1\",\"_id\":\"id1\",\"_version\":1}">>}}
erlasticsearch@pecorino)7> erlasticsearch:insert_doc(<<"index2">>, <<"type3">>, <<"id2">>, <<"{\"some_key\":\"some_val\"}">>, [{'_ttl', '1d'}]). 
{ok,{restResponse,201,undefined,
                  <<"{\"ok\":true,\"_index\":\"index2\",\"_type\":\"type3\",\"_id\":\"id2\",\"_version\":1}">>}}
erlasticsearch@pecorino)8> erlasticsearch:insert_doc(<<"index3">>, <<"type3">>, undefined, <<"{\"some_key\":\"some_val\"}">>).
{ok,{restResponse,201,undefined,
                  <<"{\"ok\":true,\"_index\":\"index3\",\"_type\":\"type3\",\"_id\":\"8Ji9R-TtT4KXxUOvb14K8g\",\"_version\":1}">>}}
```
```erlang
erlasticsearch@pecorino)9> erlasticsearch:get_doc(<<"index1">>, <<"type1">>, <<"id1">>).
{ok,{restResponse,200,undefined,
                  <<"{\"_index\":\"index1\",\"_type\":\"type1\",\"_id\":\"id1\",\"_version\":1,\"exists\":true, \"_source\" : {\"som"...>>}}
erlasticsearch@pecorino)10> erlasticsearch:get_doc(<<"index1">>, <<"type1">>, <<"id1">>, [{fields, foobar}]).
{ok,{restResponse,200,undefined,
                  <<"{\"_index\":\"index1\",\"_type\":\"type1\",\"_id\":\"id1\",\"_version\":1,\"exists\":true}">>}}
erlasticsearch@pecorino)11> erlasticsearch:get_doc(<<"index1">>, <<"type1">>, <<"id1">>, [{fields, some_key}]).
{ok,{restResponse,200,undefined,
                  <<"{\"_index\":\"index1\",\"_type\":\"type1\",\"_id\":\"id1\",\"_version\":1,\"exists\":true,\"fields\":{\"some_ke"...>>}}
```
```erlang
erlasticsearch@pecorino)12> erlasticsearch:delete_doc(<<"index1">>, <<"type1">>, <<"id1">>).                   
{ok,{restResponse,200,undefined,
                  <<"{\"ok\":true,\"found\":true,\"_index\":\"index1\",\"_type\":\"type1\",\"_id\":\"id1\",\"_version\":2}">>}}
```

Credits
=======
(Not to be confused with [erlastic_search](https://github.com/tsloughter/erlastic_search) by tsloughter, which is HTTP/REST based, and quite probably more feature rich)

(Yes, this is a _Credit_)
