// locate previous DOM node, take innerHTML as non-html input
var thisScript=document.currentScript;
var textDiv=thisScript.previousElementSibling;
var textpre=textDiv.childNodes[0];
var diagramText=textpre.innerText||textDiv.textContext;
// create diagram from text
textDiv.innerHTML="";
var view = new vega.View(vega.parse(diagramText))
    .render('svg')
    .initialize(textDiv)
    .hover()
    .run();

