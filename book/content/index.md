---
date: 2017-05-19T21:52:39
title: rundgl
type: index
weight: 0
---

This is a guide for how to use the rundgl - graph data loading asset brought to you by the Vanguard team.

### Motivation

A common challenge in early stage graph deals is to prove out the ability to load data quickly into DSE Graph. The DataStax Graph Loader ([DGL](https://docs.datastax.com/en/dse/5.1/dse-dev/datastax_enterprise/graph/dgl/dglOverview.html)) is an optimized loader for DSE graph that can help achieve high performance ingests against DSE Graph.

The kinds of graphs we are focused on loading in this asset are idempotent graphs, graphs with custom IDs and single cardinality edges. More information around the motivation for this asset can be found in the two part blog series on loading billions of vertices into DSE Graph:

[Part 1 - Theory](https://www.datastax.com/dev/blog/large-graph-loading-best-practices-strategies-part-1)

[Part 2 - Practice](://www.datastax.com/dev/blog/large-graph-loading-tactics-part-2)

### What is included?

 - engine block powered data generation
 - a bash wrapper for DGL called `rundgl` which passes a list of files for the DGL to load and tracks how many files have been loaded. This is useful for loading a lot of data.
 - scripts that generate performance charts to measure vertices, edges, and properties per second.

### Business Take Aways

DSE Graph gives businesses the ability to extract value from relationships in their data in a scalabile, always on fashion. Unlike analytical graph engines which load a static graph to memory and then crunch that static graph for insights, an operational graph is constantly changing as the real world concepts whose relationships and vertices it represents are created, updated, and deleted.

For graphs that power business critical applications, are large, and are constantly changing, DSE Graph truly shines.

Key Takeaway - DSE Graph is designed as a real-time, operational, distributed graph database.

### Technical Take Aways

DGL maximizes load performance by

1) splitting out vertices, edges, and properties and loading them in sequence
2) using maps and scripts with the groovy string API to minimize the size of the reqest over the wire but at the same time batch multiple inserts in the same request
3) using parameterized queries, vertex_complete, external_vertex_verify, and vertex_unique.
