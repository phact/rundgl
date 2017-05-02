//This lets you log to logger.log
log = org.slf4j.LoggerFactory.getLogger("Mapping Script");

//File pattern including the full path
filePattern=/.+out.+csv/

//Some utility functions
//null checks
isBlank = {
 it == null || (it instanceof String && (it.isEmpty() || it.isAllWhitespace()))
}

noneBlank = { final String... args ->
 return { def m ->
  def keys = args != null && args.length > 0 ? args : m.keySet() as String[]
   return !keys.collect(m.&get).any(isBlank)
 }
}

removeBlank = null // required to allow recursion
removeBlank = { m ->
 def result = [:]
  m.keySet().each {
   def val = m[it]
    if (!isBlank(val)) {
     result.put(it, val instanceof Map ? removeBlank(val) : val)
    }
  }
 result
}

//Directory file traversal
//we are not traversing directories in this example but if you need to you must include the whole package name for
//java.ioFile
/*
import java.io.File as JavaFile
import groovy.io.FileType
*/

//Utility for matching file names with load blocks
def getFiles(fileList, matchPattern) {
 def list = []
  log.info("list:" + fileList)
  log.info("pattern:" + matchPattern)
  filearray = fileList.split(" ")
  for( String value : filearray){
   if (value ==~ matchPattern){
    log.info("value added to matched list:" + value)
    list << value
   }
  }
 list
}


//The S3 integration has performance issues, when loading large volumes avoid using S3
//for authentication install the awscli and authenticate at the OS level with aws key / secret
//when loading from S3 ensure you have a custom log4j.properties file for the DGL to avoid
//ultra verbose output
/*
def loadS3Files(fileInput, matchPattern) {
 def s3Files =[]
  new java.io.File('./data').eachLine { line ->
   s3Files << line
  }

 def list = []

  s3Files.each { file ->
   if (file ==~ matchPattern){
     list << fileInput + file
   }
  }
 list
}
*/

//Define Edges and Vertices
sampleVertex1 = {
 label "samplevertex1"
  key "sampleidv1"
}
sampleVertex2 = {
 label "samplevertex2"
  key "sampleidv2"
}
sampleEdge = {
 label "has_edge_name"

  outV "samplevertex1", {
   label "samplevertex1"
    key "sampleidv1"
  }

 inV "samplevertex2", {
  label "samplevertex2"
   key "sampleidv2"
 }
}
//These are the load blocks. Create one for each file type you want to process.
//use the getFiles utility to run the right code block for the files being passed to DGL
//the files are passed using the parameter `inputfilename` which is provided in ./rundgl
log.info("Input: " + inputfilename)
getFiles(inputfilename,  filePattern).each{file ->
  sampleInput = File.csv(file).delimiter(",")
  log.info("file :" + file)

  sampleEdgeInput = sampleInput.filter(noneBlank("sampleidv1", "sampleidv2")).transform {
//we use def to ensure that there is a copy of the map inside the script event loop. This is required
//to keep things thread safe
//we use a map inside edge definitions to provide nesting,
//the edge has properties and two vertices, each vertex has it's own properties
   def sampleEdge=[:]

    sampleEdge["prop1"]=it["prop1e"]
//you can manipulate names and values in the mapping, just make sure the name inside the it array
//matches the name of the property in your json file exactly (case sensitive)
    sampleEdge["prop2"]=it["Prop2e"]
//the vertices are defined as maps so they can hold their properties and keys
    sampleEdge["samplevertex1"]=[:]
    sampleEdge["samplevertex2"]=[:]
//the name of the key needs to match the name of the key in the vertex definition above
    sampleEdge["samplevertex1"]["sampleidv1"]=it["sampleidv1"]
    sampleEdge["samplevertex1"]["prop1"]=it["prop1v1"]
    sampleEdge["samplevertex2"]["sampleidv2"]=it["sampleidv2"]
//notice here I am hardcoding a property which will be the same for all samplevertex2's
    sampleEdge["samplevertex2"]["prop1"]="card-coded-prop"
    sampleEdge["samplevertex2"]["prop2"]=it["Prop2v2"]

    //null check for vertices
    sampleEdge["samplevertex1"] = removeBlank(sampleEdge["samplevertex1"])
    sampleEdge["samplevertex2"] = removeBlank(sampleEdge["samplevertex2"])

//return the finished map
    sampleEdge

//any empty fields will be removed
  }.transform (removeBlank)

//note, by loading this edge, you will also be loading the two Vertices
//if you have already loaded this vertices in a previous file that is not a problem, we are
//using idempotent vertices (custom IDs)
//If you already loaded a Vetex with all it's properties and you load just it's ID here, it will
//continue to have the properties on reads (cassandra writes are sparse).
  log.info("loading: sample edge")
  load(sampleEdgeInput).asEdges(sampleEdge)
//  load(sampleInput).asVertices(sampleVertex1)

}

//add other load blocks for other file types here:

