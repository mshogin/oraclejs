/*!
 * Copyright by Shogin Michael
 * See contributors list in README
 */

var occi            = require(process.env.NODEOCCI_HOME + "/node_oracle.node"); 
var utils           = require(__dirname + "/utils.js"); 
var sqlplaceholder  = require(__dirname + "/sqlplaceholder.js"); 

var OCCICHAR           = 32 *1024;     // 32768
exports.OCCICHAR       = OCCICHAR;
var OCCIDOUBLE         = 32 *1024 + 1; // 32769
exports.OCCIDOUBLE     = OCCIDOUBLE; 
var OCCIBOOL           = 32 *1024 + 2; // 32770
exports.OCCIBOOL       = OCCIBOOL;
var OCCIANYDATA        = 32 *1024 + 3; // 32771
exports.OCCIANYDATA    = OCCIANYDATA;
var OCCINUMBER         = 32 *1024 + 4; // 32772
exports.OCCINUMBER     = OCCINUMBER;
var OCCIBLOB           = 32 *1024 + 5; // 32773
exports.OCCIBLOB       = OCCIBLOB;
var OCCIBFILE          = 32 *1024 + 6; // 32774
exports.OCCIBFILE      = OCCIBFILE;
var OCCIBYTES          = 32 *1024 + 7; // 32775
exports.OCCIBYTES      = OCCIBYTES;
var OCCICLOB           = 32 *1024 + 8; // 32776
exports.OCCICLOB       = OCCICLOB;
var OCCIVECTOR         = 32 *1024 + 9; // 32777
exports.OCCIVECTOR     = OCCIVECTOR;
var OCCIMETADATA       = 32 *1024 + 10; // 32778
exports.OCCIMETADATA   = OCCIMETADATA;
var OCCIPOBJECT        = 32 *1024 + 11; // 32779
exports.OCCIPOBJECT    = OCCIPOBJECT;
var OCCIREF            = 32 *1024 + 12; // 32780
exports.OCCIREF        = OCCIREF;
var OCCIREFANY         = 32 *1024 + 13; // 32781
exports.OCCIREFANY     = OCCIREFANY;
var OCCISTRING         = 32 *1024 + 14; // 32782
exports.OCCISTRING     = OCCISTRING;
var OCCISTREAM         = 32 *1024 + 15; // 32783
exports.OCCISTREAM     = OCCISTREAM;
var OCCIDATE           = 32 *1024 + 16; // 32784
exports.OCCIDATE       = OCCIDATE;
var OCCIINTERVALDS     = 32 *1024 + 17; // 32785
exports.OCCIINTERVALDS = OCCIINTERVALDS;
var OCCIINTERVALYM     = 32 *1024 + 18; // 32786
exports.OCCIINTERVALYM = OCCIINTERVALYM;
var OCCITIMESTAMP      = 32 *1024 + 19; // 32787
exports.OCCITIMESTAMP  = OCCITIMESTAMP;
var OCCIROWID          = 32 *1024 + 20; // 32788
exports.OCCIROWID      = OCCIROWID;
var OCCICURSOR         = 32 *1024 + 21; // 32789
exports.OCCICURSOR     = OCCICURSOR;

var Connection = function(){
    this.conn = new occi.conn();

    Connection.prototype.create_statement = function(sql){
        if ( !sql )
            throw "No SQL defined";
        
        return new Statement( 
            this,
            this.conn,
            sql     
            );
    };

    Connection.prototype.exec = function(sql){
        if ( !sql )
            throw "No SQL defined";

        var st = this.create_statement(sql);
        st.execute();
        
        return this;
    };

    Connection.prototype.connect = utils.polymorph (
        function (schema, passwd, connection_string) {
            this.conn.connect(
                schema, 
                passwd, 
                connection_string, 
                'UTF8', 
                'UTF8'
            );
            
            return this;
        },
        function (schema, passwd, server, sid) {
            var conn_str = '//' + server + '/' + sid;
            
            this.conn.connect(
                schema, 
                passwd, 
                connection_string, 
                'UTF8', 
                'UTF8'
            );
            
            return this;
        },
        function (schema, passwd, server, sid, nls_db, nls_client) {
            var conn_str = '//' + server + '/' + sid;
            
            this.conn.connect(
                schema, 
                passwd, 
                connection_string, 
                nls_db, 
                nls_client
            );
            
            return this;
        }
    );

}

var Statement = function( conn, dbh, sql ) {
    this.conn = conn;
    this.dbh = dbh;
    
    var Holder = new sqlplaceholder.SQLPlaceHolder();
    this.placeholder = Holder.parse(sql);
    
    this.stmt = this.dbh.prepare(sql);

    function PrepareParam(param) {
        if ( !param )
            throw "No defined placeholder";
            
        return param.replace(':', ''); // remove first :
    }
    
    
    Statement.prototype.bind_param = function(param, value, type){
        param = PrepareParam(param);
        
        var pNum = this.placeholder[param].index;
        
        if ( type && type == OCCICLOB){
            this.stmt.bind_param(pNum, OCCICLOB, value);
            this.placeholder[param].type = OCCICLOB;
        }
        else if ( type && type == OCCISTRING){
            this.stmt.bind_param(pNum, OCCISTRING, value);
            this.placeholder[param].type = OCCISTRING;
        }
        else if ( type && type == OCCINUMBER){
            this.stmt.bind_param(pNum, OCCINUMBER, value);
            this.placeholder[param].type = OCCINUMBER;
        }
        else if ( typeof value == 'number'){
            this.stmt.bind_param(pNum, OCCINUMBER, value);
            this.placeholder[param].type = OCCINUMBER;
        }
        else if ( typeof value == 'string'){
            this.stmt.bind_param(pNum, OCCISTRING, value);
            this.placeholder[param].type = OCCISTRING;
        }
    };
    
    Statement.prototype.bind_param_inout = function(param, type, size){
        param = PrepareParam(param);
        
        var pNum = this.placeholder[param].index;

        if ( type == OCCISTRING ) {
            this.stmt.bind_param_inout(pNum, type, size);
        }   
        else {         
            this.stmt.bind_param_inout(pNum, type);
        }
        
        this.placeholder[param].type = type;
    }; 
    
    Statement.prototype.get = function(param, len){
        param = PrepareParam(param);

        with ( this.placeholder[param] ) {
            if ( type == OCCICLOB ) {
                return this.stmt.get_clob(index, len);
            }
            else if ( type == OCCISTRING ) {
                return this.stmt.get_string(index);
            }
        }
        
        return undefined;
    };
    
    Statement.prototype.execute = function(before, complete) {
        if (before)
            before.apply(this);
            
        this.stmt.execute();
        
        if (complete)
            complete.apply(this);
    };
    
    Statement.prototype.fetchall = function(param){
        param = PrepareParam(param);
        
        with ( this.placeholder[param] ) {
            if ( type != OCCICURSOR )
                throw "Need OCCICURSOR";
            
            var rs = this.stmt.get_cursor(index);
                
            return rs.fetch_all();
        }
        
        return [];
    };

    Statement.prototype.fetch = function(param){
        param = PrepareParam(param);
        
        with ( this.placeholder[param] ) {
            if ( type != OCCICURSOR )
                throw "Need OCCICURSOR";
            
            var rs = this.stmt.get_cursor(index);
                
            return rs.fetch();
        }
        
        return undefined;
    };

}

exports.Connection = Connection;

