function refreshStat()
{
    $('#schdlr_stat').load('stat');
}
var auto_refresh = setInterval(refreshStat, 500); // refresh every 500 milliseconds

var topnav = document.getElementById("topnav");
var btns = topnav.getElementsByTagName("a");

for (var i = 0; i < btns.length; i++) {
  btns[i].addEventListener("click", function() {
      var current = document.getElementsByClassName(" active")[0];
      current.className = "";
      this.className = "active";
    }
  );
}