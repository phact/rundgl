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

    sampleEdge["prop1e"]=it["prop1e"]
//you can manipulate names and values in the mapping, just make sure the name inside the it array
//matches the name of the property in your json file exactly (case sensitive)
    sampleEdge["Prop2e"]=it["Prop2e"]
//the vertices are defined as maps so they can hold their properties and keys
    sampleEdge["samplevertex1"]=[:]
    sampleEdge["samplevertex2"]=[:]
//the name of the key needs to match the name of the key in the vertex definition above
    sampleEdge["samplevertex1"]["sampleidv1"]=it["sampleidv1"]
    sampleEdge["samplevertex2"]["sampleidv2"]=it["sampleidv2"]
//notice here I am hardcoding a property which will be the same for all samplevertex2's
    sampleEdge["samplevertex2"]["Prop2v2"]=it["Prop2v2"]
    sampleEdge["samplevertex2"]["prop1v2"]=it["prop1v2"]
    sampleEdge["samplevertex2"]["prop2v2"]=it["prop2v2"]
    sampleEdge["samplevertex2"]["prop3v2"]=it["prop3v2"]
    sampleEdge["samplevertex2"]["prop4v2"]=it["prop4v2"]
    sampleEdge["samplevertex2"]["prop5v2"]=it["prop5v2"]
    sampleEdge["samplevertex2"]["prop6v2"]=it["prop6v2"]
    sampleEdge["samplevertex2"]["prop7v2"]=it["prop7v2"]
    sampleEdge["samplevertex2"]["prop8v2"]=it["prop8v2"]
    sampleEdge["samplevertex2"]["prop9v2"]=it["prop9v2"]
    sampleEdge["samplevertex2"]["prop10v2"]=it["prop10v2"]
    sampleEdge["samplevertex2"]["prop11v2"]=it["prop11v2"]
    sampleEdge["samplevertex2"]["prop12v2"]=it["prop12v2"]
    sampleEdge["samplevertex2"]["prop13v2"]=it["prop13v2"]
    sampleEdge["samplevertex2"]["prop14v2"]=it["prop14v2"]
    sampleEdge["samplevertex2"]["prop15v2"]=it["prop15v2"]
    sampleEdge["samplevertex2"]["prop16v2"]=it["prop16v2"]
    sampleEdge["samplevertex2"]["prop17v2"]=it["prop17v2"]
    sampleEdge["samplevertex2"]["prop18v2"]=it["prop18v2"]
    sampleEdge["samplevertex2"]["prop19v2"]=it["prop19v2"]
    sampleEdge["samplevertex2"]["prop20v2"]=it["prop20v2"]
    sampleEdge["samplevertex2"]["prop21v2"]=it["prop21v2"]
    sampleEdge["samplevertex2"]["prop22v2"]=it["prop22v2"]
    sampleEdge["samplevertex2"]["prop23v2"]=it["prop23v2"]
    sampleEdge["samplevertex2"]["prop24v2"]=it["prop24v2"]
    sampleEdge["samplevertex2"]["prop25v2"]=it["prop25v2"]
    sampleEdge["samplevertex2"]["prop26v2"]=it["prop26v2"]
    sampleEdge["samplevertex2"]["prop27v2"]=it["prop27v2"]
    sampleEdge["samplevertex2"]["prop28v2"]=it["prop28v2"]
    sampleEdge["samplevertex2"]["prop29v2"]=it["prop29v2"]
    sampleEdge["samplevertex2"]["prop30v2"]=it["prop30v2"]
    sampleEdge["samplevertex2"]["prop31v2"]=it["prop31v2"]
    sampleEdge["samplevertex2"]["prop32v2"]=it["prop32v2"]
    sampleEdge["samplevertex2"]["prop33v2"]=it["prop33v2"]
    sampleEdge["samplevertex2"]["prop34v2"]=it["prop34v2"]
    sampleEdge["samplevertex2"]["prop35v2"]=it["prop35v2"]
    sampleEdge["samplevertex2"]["prop36v2"]=it["prop36v2"]
    sampleEdge["samplevertex2"]["prop37v2"]=it["prop37v2"]
    sampleEdge["samplevertex2"]["prop38v2"]=it["prop38v2"]
    sampleEdge["samplevertex2"]["prop39v2"]=it["prop39v2"]
    sampleEdge["samplevertex2"]["prop40v2"]=it["prop40v2"]
    sampleEdge["samplevertex2"]["prop41v2"]=it["prop41v2"]
    sampleEdge["samplevertex2"]["prop42v2"]=it["prop42v2"]
    sampleEdge["samplevertex2"]["prop43v2"]=it["prop43v2"]
    sampleEdge["samplevertex2"]["prop44v2"]=it["prop44v2"]
    sampleEdge["samplevertex2"]["prop45v2"]=it["prop45v2"]
    sampleEdge["samplevertex2"]["prop46v2"]=it["prop46v2"]
    sampleEdge["samplevertex2"]["prop47v2"]=it["prop47v2"]
    sampleEdge["samplevertex2"]["prop48v2"]=it["prop48v2"]
    sampleEdge["samplevertex2"]["prop49v2"]=it["prop49v2"]
    sampleEdge["samplevertex2"]["prop50v2"]=it["prop50v2"]
    sampleEdge["samplevertex2"]["prop51v2"]=it["prop51v2"]
    sampleEdge["samplevertex2"]["prop52v2"]=it["prop52v2"]
    sampleEdge["samplevertex2"]["prop53v2"]=it["prop53v2"]
    sampleEdge["samplevertex2"]["prop54v2"]=it["prop54v2"]
    sampleEdge["samplevertex2"]["prop55v2"]=it["prop55v2"]
    sampleEdge["samplevertex2"]["prop56v2"]=it["prop56v2"]
    sampleEdge["samplevertex2"]["prop57v2"]=it["prop57v2"]
    sampleEdge["samplevertex2"]["prop58v2"]=it["prop58v2"]
    sampleEdge["samplevertex2"]["prop59v2"]=it["prop59v2"]
    sampleEdge["samplevertex2"]["prop60v2"]=it["prop60v2"]
    sampleEdge["samplevertex2"]["prop61v2"]=it["prop61v2"]
    sampleEdge["samplevertex2"]["prop62v2"]=it["prop62v2"]
    sampleEdge["samplevertex2"]["prop63v2"]=it["prop63v2"]
    sampleEdge["samplevertex2"]["prop64v2"]=it["prop64v2"]
    sampleEdge["samplevertex2"]["prop65v2"]=it["prop65v2"]
    sampleEdge["samplevertex2"]["prop66v2"]=it["prop66v2"]
    sampleEdge["samplevertex2"]["prop67v2"]=it["prop67v2"]
    sampleEdge["samplevertex2"]["prop68v2"]=it["prop68v2"]
    sampleEdge["samplevertex2"]["prop69v2"]=it["prop69v2"]
    sampleEdge["samplevertex2"]["prop70v2"]=it["prop70v2"]
    sampleEdge["samplevertex2"]["prop71v2"]=it["prop71v2"]
    sampleEdge["samplevertex2"]["prop72v2"]=it["prop72v2"]
    sampleEdge["samplevertex2"]["prop73v2"]=it["prop73v2"]
    sampleEdge["samplevertex2"]["prop74v2"]=it["prop74v2"]
    sampleEdge["samplevertex2"]["prop75v2"]=it["prop75v2"]
    sampleEdge["samplevertex2"]["prop76v2"]=it["prop76v2"]
    sampleEdge["samplevertex2"]["prop77v2"]=it["prop77v2"]
    sampleEdge["samplevertex2"]["prop78v2"]=it["prop78v2"]
    sampleEdge["samplevertex2"]["prop79v2"]=it["prop79v2"]
    sampleEdge["samplevertex2"]["prop80v2"]=it["prop80v2"]
    sampleEdge["samplevertex2"]["prop81v2"]=it["prop81v2"]
    sampleEdge["samplevertex2"]["prop82v2"]=it["prop82v2"]
    sampleEdge["samplevertex2"]["prop83v2"]=it["prop83v2"]
    sampleEdge["samplevertex2"]["prop84v2"]=it["prop84v2"]
    sampleEdge["samplevertex2"]["prop85v2"]=it["prop85v2"]
    sampleEdge["samplevertex2"]["prop86v2"]=it["prop86v2"]
    sampleEdge["samplevertex2"]["prop87v2"]=it["prop87v2"]
    sampleEdge["samplevertex2"]["prop88v2"]=it["prop88v2"]
    sampleEdge["samplevertex2"]["prop89v2"]=it["prop89v2"]
    sampleEdge["samplevertex2"]["prop90v2"]=it["prop90v2"]
    sampleEdge["samplevertex2"]["prop91v2"]=it["prop91v2"]
    sampleEdge["samplevertex2"]["prop92v2"]=it["prop92v2"]
    sampleEdge["samplevertex2"]["prop93v2"]=it["prop93v2"]
    sampleEdge["samplevertex2"]["prop94v2"]=it["prop94v2"]
    sampleEdge["samplevertex2"]["prop95v2"]=it["prop95v2"]
    sampleEdge["samplevertex2"]["prop96v2"]=it["prop96v2"]
    sampleEdge["samplevertex2"]["prop97v2"]=it["prop97v2"]
    sampleEdge["samplevertex2"]["prop98v2"]=it["prop98v2"]
    sampleEdge["samplevertex2"]["prop99v2"]=it["prop99v2"]
    sampleEdge["samplevertex2"]["prop100v2"]=it["prop100v2"]
    sampleEdge["samplevertex2"]["prop101v2"]=it["prop101v2"]
    sampleEdge["samplevertex2"]["prop102v2"]=it["prop102v2"]
    sampleEdge["samplevertex2"]["prop103v2"]=it["prop103v2"]
    sampleEdge["samplevertex2"]["prop104v2"]=it["prop104v2"]
    sampleEdge["samplevertex2"]["prop105v2"]=it["prop105v2"]
    sampleEdge["samplevertex2"]["prop106v2"]=it["prop106v2"]
    sampleEdge["samplevertex2"]["prop107v2"]=it["prop107v2"]
    sampleEdge["samplevertex2"]["prop108v2"]=it["prop108v2"]
    sampleEdge["samplevertex2"]["prop109v2"]=it["prop109v2"]
    sampleEdge["samplevertex2"]["prop110v2"]=it["prop110v2"]
    sampleEdge["samplevertex2"]["prop111v2"]=it["prop111v2"]
    sampleEdge["samplevertex2"]["prop112v2"]=it["prop112v2"]
    sampleEdge["samplevertex2"]["prop113v2"]=it["prop113v2"]
    sampleEdge["samplevertex2"]["prop114v2"]=it["prop114v2"]
    sampleEdge["samplevertex2"]["prop115v2"]=it["prop115v2"]
    sampleEdge["samplevertex2"]["prop116v2"]=it["prop116v2"]
    sampleEdge["samplevertex2"]["prop117v2"]=it["prop117v2"]
    sampleEdge["samplevertex2"]["prop118v2"]=it["prop118v2"]
    sampleEdge["samplevertex2"]["prop119v2"]=it["prop119v2"]
    sampleEdge["samplevertex2"]["prop120v2"]=it["prop120v2"]
    sampleEdge["samplevertex2"]["prop121v2"]=it["prop121v2"]
    sampleEdge["samplevertex2"]["prop122v2"]=it["prop122v2"]
    sampleEdge["samplevertex2"]["prop123v2"]=it["prop123v2"]
    sampleEdge["samplevertex2"]["prop124v2"]=it["prop124v2"]
    sampleEdge["samplevertex2"]["prop125v2"]=it["prop125v2"]
    sampleEdge["samplevertex2"]["prop126v2"]=it["prop126v2"]
    sampleEdge["samplevertex2"]["prop127v2"]=it["prop127v2"]
    sampleEdge["samplevertex2"]["prop128v2"]=it["prop128v2"]
    sampleEdge["samplevertex2"]["prop129v2"]=it["prop129v2"]
    sampleEdge["samplevertex2"]["prop130v2"]=it["prop130v2"]
    sampleEdge["samplevertex2"]["prop131v2"]=it["prop131v2"]
    sampleEdge["samplevertex2"]["prop132v2"]=it["prop132v2"]
    sampleEdge["samplevertex2"]["prop133v2"]=it["prop133v2"]
    sampleEdge["samplevertex2"]["prop134v2"]=it["prop134v2"]
    sampleEdge["samplevertex2"]["prop135v2"]=it["prop135v2"]
    sampleEdge["samplevertex2"]["prop136v2"]=it["prop136v2"]
    sampleEdge["samplevertex2"]["prop137v2"]=it["prop137v2"]
    sampleEdge["samplevertex2"]["prop138v2"]=it["prop138v2"]
    sampleEdge["samplevertex2"]["prop139v2"]=it["prop139v2"]
    sampleEdge["samplevertex2"]["prop140v2"]=it["prop140v2"]
    sampleEdge["samplevertex2"]["prop141v2"]=it["prop141v2"]
    sampleEdge["samplevertex2"]["prop142v2"]=it["prop142v2"]
    sampleEdge["samplevertex2"]["prop143v2"]=it["prop143v2"]
    sampleEdge["samplevertex2"]["prop144v2"]=it["prop144v2"]
    sampleEdge["samplevertex2"]["prop145v2"]=it["prop145v2"]
    sampleEdge["samplevertex2"]["prop146v2"]=it["prop146v2"]
    sampleEdge["samplevertex2"]["prop147v2"]=it["prop147v2"]
    sampleEdge["samplevertex2"]["prop148v2"]=it["prop148v2"]
    sampleEdge["samplevertex2"]["prop149v2"]=it["prop149v2"]
    sampleEdge["samplevertex2"]["prop150v2"]=it["prop150v2"]
    sampleEdge["samplevertex2"]["prop151v2"]=it["prop151v2"]
    sampleEdge["samplevertex2"]["prop152v2"]=it["prop152v2"]
    sampleEdge["samplevertex2"]["prop153v2"]=it["prop153v2"]
    sampleEdge["samplevertex2"]["prop154v2"]=it["prop154v2"]
    sampleEdge["samplevertex2"]["prop155v2"]=it["prop155v2"]
    sampleEdge["samplevertex2"]["prop156v2"]=it["prop156v2"]
    sampleEdge["samplevertex2"]["prop157v2"]=it["prop157v2"]
    sampleEdge["samplevertex2"]["prop158v2"]=it["prop158v2"]
    sampleEdge["samplevertex2"]["prop159v2"]=it["prop159v2"]
    sampleEdge["samplevertex2"]["prop160v2"]=it["prop160v2"]
    sampleEdge["samplevertex2"]["prop161v2"]=it["prop161v2"]
    sampleEdge["samplevertex2"]["prop162v2"]=it["prop162v2"]
    sampleEdge["samplevertex2"]["prop163v2"]=it["prop163v2"]
    sampleEdge["samplevertex2"]["prop164v2"]=it["prop164v2"]
    sampleEdge["samplevertex2"]["prop165v2"]=it["prop165v2"]
    sampleEdge["samplevertex2"]["prop166v2"]=it["prop166v2"]
    sampleEdge["samplevertex2"]["prop167v2"]=it["prop167v2"]
    sampleEdge["samplevertex2"]["prop168v2"]=it["prop168v2"]
    sampleEdge["samplevertex2"]["prop169v2"]=it["prop169v2"]
    sampleEdge["samplevertex2"]["prop170v2"]=it["prop170v2"]
    sampleEdge["samplevertex2"]["prop171v2"]=it["prop171v2"]
    sampleEdge["samplevertex2"]["prop172v2"]=it["prop172v2"]
    sampleEdge["samplevertex2"]["prop173v2"]=it["prop173v2"]
    sampleEdge["samplevertex2"]["prop174v2"]=it["prop174v2"]
    sampleEdge["samplevertex2"]["prop175v2"]=it["prop175v2"]
    sampleEdge["samplevertex2"]["prop176v2"]=it["prop176v2"]
    sampleEdge["samplevertex2"]["prop177v2"]=it["prop177v2"]
    sampleEdge["samplevertex2"]["prop178v2"]=it["prop178v2"]
    sampleEdge["samplevertex2"]["prop179v2"]=it["prop179v2"]
    sampleEdge["samplevertex2"]["prop180v2"]=it["prop180v2"]
    sampleEdge["samplevertex2"]["prop181v2"]=it["prop181v2"]
    sampleEdge["samplevertex2"]["prop182v2"]=it["prop182v2"]
    sampleEdge["samplevertex2"]["prop183v2"]=it["prop183v2"]
    sampleEdge["samplevertex2"]["prop184v2"]=it["prop184v2"]
    sampleEdge["samplevertex2"]["prop185v2"]=it["prop185v2"]
    sampleEdge["samplevertex2"]["prop186v2"]=it["prop186v2"]
    sampleEdge["samplevertex2"]["prop187v2"]=it["prop187v2"]
    sampleEdge["samplevertex2"]["prop188v2"]=it["prop188v2"]
    sampleEdge["samplevertex2"]["prop189v2"]=it["prop189v2"]
    sampleEdge["samplevertex2"]["prop190v2"]=it["prop190v2"]
    sampleEdge["samplevertex2"]["prop191v2"]=it["prop191v2"]
    sampleEdge["samplevertex2"]["prop192v2"]=it["prop192v2"]
    sampleEdge["samplevertex2"]["prop193v2"]=it["prop193v2"]
    sampleEdge["samplevertex2"]["prop194v2"]=it["prop194v2"]
    sampleEdge["samplevertex2"]["prop195v2"]=it["prop195v2"]
    sampleEdge["samplevertex2"]["prop196v2"]=it["prop196v2"]
    sampleEdge["samplevertex2"]["prop197v2"]=it["prop197v2"]
    sampleEdge["samplevertex2"]["prop198v2"]=it["prop198v2"]
    sampleEdge["samplevertex2"]["prop199v2"]=it["prop199v2"]
    sampleEdge["samplevertex2"]["prop200v2"]=it["prop200v2"]

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

