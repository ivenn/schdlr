function refreshSchedulerStat()
{
    $('#scheduler_stat').load('scheduler_stat');
}

function refreshWorkflowStat()
{
    $('#workflow_stat').load('workflow_stat');
}

var auto_refresh1 = setInterval(refreshSchedulerStat, 1000); // refresh every 1000 milliseconds
var auto_refresh2 = setInterval(refreshWorkflowStat, 1000); // refresh every 1000 milliseconds

var topnav = document.getElementById("topnav");
var btns = topnav.getElementsByTagName("a");

//for (var i = 0; i < btns.length; i++) {
//  btns[i].addEventListener("click", function() {
//      var current = document.getElementsByClassName("active");
//      current[0].className = current[0].className.replace(" active", "");
//      this.className += " active";
//    }
//  );
//}
