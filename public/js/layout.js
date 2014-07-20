function layout()
{

	// get browser size
	var s = detectBrowserSize();

	// set body
	$("body").width(s.w-20).height(s.h);

	var head = $("#header-container").outerHeight();
	var body = s.h-head;

	// set content and its mask
	$("#content").css("min-height",body).css("top",head-2);
	$("#content-mask").css("min-height",body).css("top",head-2).width($("#content").width()).height($("#content").height());

	// set example style
	$(".option-container").last().addClass("option-container-last");

	$("#help-container").css("left",($("#container").innerWidth() - $("#help-container").outerWidth())/2);

	check_style();
}
function check_style()
{
	var isMacLike = navigator.userAgent.match(/(Mac|iPhone|iPod|iPad)/i)?true:false;

	if(isMacLike)
	{
		$('#header-container').addClass('header-container-apple');
		// $('.cluster-odd').addClass('cluster-odd-apple');
		// $('.cluster-even').addClass('cluster-even-apple');
		// console.log(isMacLike);
	}	
}