/*!
 * Copyright by Shogin Michael
 * See contributors list in README
 */

var SQLPlaceHolder = function(){
    this.holder = {};
}

SQLPlaceHolder.prototype = {
    parse: function(sql){
        sql = sql.replace(/\s+/g, ' ').toLowerCase();
        
        var index = 1;
        for( var i=0; i < sql.length; i++ ){
            var ch = sql.charAt(i);
            
            if ( ch == '\'' ){
                i = sql.indexOf('\'', i+1) + 1;
            }
            else if ( ch == ':') {
                var j = i + 1;
                if ( sql.charAt(j) == '=' )
                    continue;
                
                var placeholder = '';
                while ( i < j ){
                    var tmp = sql.charAt(j);

                    if ( tmp.match(/[a-z0-9_]/) ){
                        placeholder += tmp;
                        j++;
                    }
                    else {
                        i = j;
                    }
                }
                
                if ( placeholder.length > 0 ){
                    this.holder[placeholder] = {
                        index: index++,
                        type: undefined
                    };
                }
            }
        }
        
        return this.holder;
    }
}

exports.SQLPlaceHolder = SQLPlaceHolder;
