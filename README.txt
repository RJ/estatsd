estatsd is a simple stats aggregation service that periodically dumps data to
Graphite: http://graphite.wikidot.com/

NB: Graphite is good, despite the website being a bit ghetto.

Inspired heavily by etsy statsd:
http://codeascraft.etsy.com/2011/02/15/measure-anything-measure-everything/

QUICK DEMO
==========

1) Install and configure graphite (quick-ish)
2) Install rebar, have it in your path
3) rebar compile
4) erl -pa ebin
5) > application:start(estatsd).
   > estatsd:increment(foo, 123).
6) Observe graphite now has 1 data point.

USAGE
=====

Add this app to your rebar deps, and make sure it's started somehow
eg: application:start(estatsd).

You can configure custom graphite host/port and flush interval using 
application environment vars. See estatsd_sup for details.

The following calls to estatsd are all gen_server:cast, ie non-blocking.

Counters
--------

    estatsd:increment(num_foos).            %% increment num_foos by one

    estatsd:decrement(<<"num_bars">>, 3).   %% increment num_bars by 3

    estatsd:increment("tcp.bytes_in", 512). %% increment tcp.bytes_in by 512

Timers
------

    estatsd:timing(sometask, 1534).         %% report that sometask took 1534ms

Or for your convenience: 

    Start = erlang:now(),
    do_sometask(), 
    estatsd:timing(sometast, Start).        %% uses now() and now_diff for you

Sampling
--------

Only report 10% of some_frequent_task measurements:

    estatsd:timing(some_frequent_task, 12, 0.1) 



NOTES
=====

This could be extended to take a callback for reporting mechanisms.
Right now it's hardcoded to stick data into graphite.



Richard Jones <rj@metabrew.com>
@metabrew
