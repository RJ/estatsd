ESTATSD
==========
StatsD, a tool developed by etsy to track statistics, has become incredibly popular in web services
for its ease of use and its numerous appliactions. estatsd, a project initially created by Richard Jones,
was an attempt to develop a StatsD server as an Erlang application for easy integration with other Erlang
projects.

With 37 forks at the time of this writing, there's no shortage of usable variants of estatsd. What sets this one
apart from the others is its emphasis on distribution and scalability.

This flavor of estatsd is designed to run on multiple systems within a local network, across datacenters, as
a single embedded application, or a standalone UDP server. This allows you to *horizontally scale* your statsd 
setup with the rest of your service.

HOW IT WORKS
==========
estatsd works on the concept of a cluster, or a local network, and uses Erlang's built-in support for distribrution
towards this end. Data being tracked by servers in the cluster is aggregated to a single, elected server (using
gen_leader; this election process makes the service more resilient in the case of network or server failure), and
is then sent on to its destination.

estatsd supports 3 types of destinations now and can easily be extended to target more. The types are:

1) graphite (of course) - The standard StatsD/estatsd endpoint
2) estatsd_tcp          - A JSON-based aggregation protocol
3) estatsd_tcpz         - Similar to estatsd_tcp, but also uses zlib to compress messages when sending

Using the estatsd_tcp and estatsd_tcpz protocols you can easily aggregate statistics from multiple datcenters into
a single graphite instance without the increased risk of lost messages that comes with UDP-repeater setups.

AND THERE'S MORE
=========
Tagging. You can apply arbitrary tags to keys (based on a regular expression) at two points: the node level, before
the stats are aggregated to the local network's master, or the cluster level, before the stats are forwarded to the
specified destination.

That's a little vague, so here's an example:

    % /etc/estatsd/cluster.config
    [
        {estatsd, [
                {cluster_tagging, [
                            {".*", copy, prefix, "my_dc_a"},
                            {".*", replace, prefix, "my_product"}
                    ]}
            ]}
    ]

Suppose you have a statistic, foo, that you're tracking. When estatsd sends this data on to its destination, you'll
receive two metrics instead of one.

    my_product.my_dc_a.foo      27
    my_product.foo              27

Suppose you had another datacenter, my_dc_b, that was also sending data, but with the tag my_dc_b. You'll receive two
two metrics again.

    my_product.my_dc_b.foo      23
    my_product.foo              23

See where I'm going with this yet? Assuming that your destination is another estatsd instance, these stats will be
aggregated, and you'll be left with *3* keys:

    my_product.my_dc_a.foo      27
    my_product.my_dc_b.foo      23
    my_product.foo              50

Intrigued yet?

QUICK DEMO
==========

1) Install and configure graphite (quick-ish)
2) Install rebar, have it in your path
3) rebar compile
4) erl -pa ebin
5) > application:start(estatsd).
   > estatsd:increment(foo, 123).
6) Observe graphite now has 1 data point.

STANDALONE SERVICE
==========

Use [estatsd_server](https://github.com/fauxsoup/estatsd_server) to generate a standalone version of estatsd.

USAGE
=====

Add this app to your rebar deps, and make sure it's started somehow
eg: application:start(estatsd).

You can configure custom graphite host/port and flush interval using 
application environment vars. See estatsd_sup for details.

The following calls to estatsd are all gen_server:cast, ie non-blocking.

Gauges
--------

    estatsd:gauge(temperature, 45).            %% set temperature to 45

Counters
--------

    estatsd:increment(num_foos).            %% increment num_foos by one

    estatsd:decrement(<<"num_bars">>, 3).   %% decrement num_bars by 3

    estatsd:increment("tcp.bytes_in", 512). %% increment tcp.bytes_in by 512

Timers
------

    estatsd:timing(sometask, 1534).         %% report that sometask took 1534ms

Or for your convenience: 

    Start = erlang:now(),
    do_sometask(), 
    estatsd:timing(sometast, Start).        %% uses now() and now_diff for you



Soup <fauxsoup@gmail.com>
