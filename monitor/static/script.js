function refreshSchedulerStat()
{
    $('#scheduler_stat').load('scheduler_stat');
}

function refreshWorkflowStat()
{
    $('#workflow_stat').load('workflow_stat');
}

function refreshCron()
{
    $('#cron').load('cron_info');
}

var auto_refresh1 = setInterval(refreshSchedulerStat, 1000); // refresh every 1000 milliseconds
var auto_refresh2 = setInterval(refreshWorkflowStat, 1000); // refresh every 1000 milliseconds
var auto_refresh3 = setInterval(refreshCron, 5000); // refresh every 5000 milliseconds

var topnav = document.getElementById("topnav");
var btns = topnav.getElementsByTagName("a");

