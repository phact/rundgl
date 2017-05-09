//stick the RPC_ADRESS in /etc/dse/graph/gremlin-console/conf/remote.yaml
//then run cat schema.groovy|dse gremlin-console
system.graph('genericgraph').ifNotExists().create()
:remote config alias g genericgraph.g

schema.propertyKey("Prop2v2").Text().single().create()
schema.propertyKey("sampleidv2").Text().single().create()
schema.propertyKey("Prop2e").Text().single().create()
schema.propertyKey("sampleidv1").Text().single().create()
schema.propertyKey("prop1e").Text().single().create()
schema.edgeLabel("has_edge_name").single().properties("Prop2e", "prop1e").create()
schema.vertexLabel("samplevertex1").partitionKey("sampleidv1").properties("prop1e").create()
schema.vertexLabel("samplevertex2").partitionKey("sampleidv2").properties("Prop2v2").create()
schema.edgeLabel("has_edge_name").connection("samplevertex1", "samplevertex2").add()
