// var ie = (function(){

//     var undef,
//         v = 3,
//         div = document.createElement('div'),
//         all = div.getElementsByTagName('i');

//     while (
//         div.innerHTML = '<!--[if gt IE ' + (++v) + ']><i></i><![endif]-->',
//         all[0]
//     );

//     return v > 4 ? v : undef;

// }());

function detectBrowserSize() {
    var myWidth = 0, myHeight = 0;
    if (typeof (window.innerWidth) == 'number') {
        //Non-IE
        myWidth = window.innerWidth;
        myHeight = window.innerHeight;
    } else if (document.documentElement && (document.documentElement.clientWidth ||   document.documentElement.clientHeight)) {
        //IE 6+ in 'standards compliant mode'
        myWidth = document.documentElement.clientWidth;
        myHeight = document.documentElement.clientHeight;
    } else if (document.body && (document.body.clientWidth || document.body.clientHeight)) {
        //IE 4 compatible
        myWidth = document.body.clientWidth;
        myHeight = document.body.clientHeight;
    }
    return {"h":myHeight, "w":myWidth,"height":myHeight, "width":myWidth}
}
function detectBrowswer()
{
    // browsers = {"chrome":false,"safari":false,"IE9":false,"IE10":false,"firefox":false};

    var browser = "IE9"; // default
    var version = $.browser.version;

    if($.browser.chrome)
        browser = "chrome";
    else if ($.browser.safari)
        browser = "safari";
    else if ($.browser.mozilla)
        browser = "firefox";
    else if ($.browser.msie)
    {
        browser = "msie";
    }
    return { "browser":browser, "version":version };
}



function thousandSign(n)
{
    var digit = n.length;
    if(digit>4)
    {
        var arr = new Array();
        var i = digit - 1;
        while(1)
        {
            arr.push(n[i]);
            arr.push(n[i-1]);
            arr.push(n[i-2]);
            if(i-3 >= 0)
            {
                i = i - 3;
                arr.push(",");
            }else
            {
                break;
            }
        }
        return arr.reverse().join("")
    }else
    {
        return n;
    }
}


// Counter
(function($) {
    $.fn.countTo = function(options) {
        // merge the default plugin settings with the custom options
        options = $.extend({}, $.fn.countTo.defaults, options || {});

        // how many times to update the value, and how much to increment the value on each update
        var loops = Math.ceil(options.speed / options.refreshInterval),
            increment = (options.to - options.from) / loops;

        return $(this).each(function() {
            var _this = this,
                loopCount = 0,
                value = options.from,
                interval = setInterval(updateTimer, options.refreshInterval);

            function updateTimer() {
                value += increment;
                loopCount++;
                $(_this).html(value.toFixed(options.decimals));

                if (typeof(options.onUpdate) == 'function') {
                    options.onUpdate.call(_this, value);
                }

                if (loopCount >= loops) {
                    clearInterval(interval);
                    value = options.to;

                    if (typeof(options.onComplete) == 'function') {
                        options.onComplete.call(_this, value);
                    }
                }
            }
        });
    };

    $.fn.countTo.defaults = {
        from: 0,  // the number the element should start at
        to: 100,  // the number the element should end at
        speed: 1000,  // how long it should take to count between the target numbers
        refreshInterval: 100,  // how often the element should be updated
        decimals: 0,  // the number of decimal places to show
        onUpdate: null,  // callback method for every time the element is updated,
        onComplete: null,  // callback method for when the element finishes updating
    };
})(jQuery);