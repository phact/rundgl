
date = "2017-04-10T14:21:00-05:00"
title = "Architecture"
weight = 3
+++

This section details the architecture demonstrated in this reference field asset.

### Architecture Diagram

<div title="rendered dynamically" align="middle">
{{< mermaid >}}
graph LR
J["JSON"]
C["CSV"]
A["Graphson"]
R["RDBMS"]
B["..."]
H["HDFS"]
G["DSE Graph"]
S["rundgl script"]
M["Groovy Mapper"]--"Business Logic"-->D
J--"input"-->D["DGL"]
C--"input"-->D
H--"input"-->D
R--"input"-->D
A--"input"-->D
B--"input"-->D
S--"executes"-->D
D--"bulk loading"-->G
{{< /mermaid >}}
</div>

The DGL is a single process, multi-threaded app, that can take data from multiple sources and using business logic defined in a groovy mapping file, load the data in a bulk fashion into DSE Graph.
