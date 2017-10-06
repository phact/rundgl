// locate previous DOM node, take innerHTML as non-html input
var thisScript=document.currentScript;
var textDiv=thisScript.previousElementSibling;
var textpre=textDiv.childNodes[0];
var diagramText=textpre.innerText||textDiv.textContext;
// create diagram from text
var diagram=flowchart.parse(diagramText);
// insert new after text node.
// // var diagramDiv=document.createElement('div');
textDiv.innerHTML="";
// // textDiv.parentNode.insertBefore(diagramDiv,textDiv.nextSibling);
diagram.drawSVG(textDiv);
//textDiv.style.visibility='hidden';

