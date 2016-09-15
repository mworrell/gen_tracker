gen_tracker
-----------

gen_tracker is an alternative supervisor, that allows you to register children by term in named ets table,
store their metadata in this table and clean their data if they die.

It has reduced functionality, comparing to OTP supervisor, it doesn't have restart throttling.

However, it supports additional features like Module:after_terminate callback which is called with
name and metadata after child is dead.

gen_tracker can help you to find a child by its name via blazing fast ets:lookup call.


License
=======

MIT license. Use as you wish, don't remove copyrights and pull requests, please.


Usage
=====


Include new supervisor in your application:

    flussonic_sup.erl:
    
    init([]) ->
      Supervisors = [{streams, {gen_tracker, start_link, [streams]}, permanent, infinity, supervisor, []}],
      {ok, { {one_for_one, 5, 10}, Supervisors} }.

Now gen_tracker instance is started and ets tables 'streams' and 'streams_attrs' are created. Be sure that this name doesn't mess with anything else.
Also this gen_tracker instance automatically registers itself as a 'streams' process. It is a good idea,
because table 'streams' is created as a public,named_table so it is already a singleton.


Now comes process adding:

    Stream = {<<"tv1">>, {stream, start_link, [<<"tv1">>, Options]}, temporary, infinity, supervisor, []},
    {ok, Pid} = gen_tracker:find_or_open(streams, Stream).


gen_tracker instance called 'streams' atomically either find existing child with name <<"tv1">>, either creates
new instance and registeres it in table 'streams'.


Now you can find it:

    {ok, Pid} = gen_tracker:find(streams, <<"tv1">>).


This function call doesn't make any gen_server:call to any process, only ets table lookup is performed

Now let's work with metadata. Process launched under gen_tracker can save some metadata outside itself so
that any other process can access it without making blocking and expensive gen_server:call:

    gen_tracker:setattr(streams, Name, [{hds,true},{bytes_in,0},{bytes_out,0}]),
    gen_tracker:increment(streams, Name, bytes_in, 1000),
    {ok, BytesIn} = gen_tracker:getattr(streams, Name, bytes_in).

Now let's be sure that gen_tracker instance is a supervisor:

    supervisor:which_children(streams),
    supervisor:delete_child(streams, <<"tv1">>).



You can add existing process to supervisor tree.

    gen_tracker:add_existing_child(streams, {<<"stream1">>, Pid, worker, []}).

This can be used for example for adding websocket handlers to some trackers.


If a process is terminated, and it was started with a {M,F,A} specification then the module can export
after_terminate/2 or after_terminate/4. After process termination and cleanup they are called as:

    M:after_terminate(Name, Attrs)
    M:after_terminate(Name, Attrs, MFA, Reason)


Options
-------

The gen_tracker has an optional callback module. This module is passed during initialization:

    init([]) ->
      Options = [ {callback_module, my_streams_callback} ],
      Supervisors = [{streams, {gen_tracker, start_link, [streams, Options]}, permanent, infinity, supervisor, []}],
      {ok, { {one_for_one, 5, 10}, Supervisors} }.


The callbacks are:

    after_add_child(Zone, Name, MFA, Result)
    after_terminate(Zone, Name, Attrs, MFA, Reason)

MFA is undefined for children added using add_existing_child/2.
