<?php
namespace fmihel\base\test;

use PHPUnit\Framework\TestCase;
use fmihel\base\{Base,BaseException};
use fmihel\console;

define('TABLE_FILL','test_clients');
define('TABLE_EMPTY','test_clients_phone');

// tables  ---------------
// test_clients - is filled
// test_clients_phone - is empty
//  ------------------------

const DS_ROWS  = [
    ['id'=>1,"NAME"=>'bbbb',"AGE"=>2,'ID_CLIENT'=>1],
    ['id'=>2,"NAME"=>'Soma',"AGE"=>43,'ID_CLIENT'=>2],
    ['id'=>3,"NAME"=>'Keks',"AGE"=>78,'ID_CLIENT'=>3],
    ['id'=>4,"NAME"=>'Pretor',"AGE"=>5,'ID_CLIENT'=>4],

];

final class BaseTest extends TestCase{

    public static function setUpBeforeClass(): void
    {
        require_once(__DIR__.'/data/configBase.php');
        Base::connect($config);
        
        foreach(DS_ROWS as $row){
            $q = Base::generate('insertOnDuplicate',TABLE_FILL,$row,
                ['types'=>Base::getTypes(TABLE_FILL,'test'),'exclude'=>'id']
            );
            Base::query($q,'test','utf8');
        };
    }    
    public function test_connect(){
        $connect = Base::connect('test');
        self::assertTrue( $connect === true);
    }

    /**
     * @depends test_connect
     */    
    public function test_query(){
        $q = 'select * from '.TABLE_FILL;
        $res = Base::query($q,'test');
        //error_log(print_r($res,true));
        self::assertTrue( $res!==false );

         // --------------------------------------------
         $this->expectException(\Exception::class);
         $res = Base::query($q,'test2');
         // --------------------------------------------
    }
    /**
     * @depends test_ds
     */    
    public function test_fields(){
        // ------------------------------
        // charset is not set
        $q = 'select * from '.TABLE_FILL;
        $ds = Base::ds($q,'test');
        $fields = Base::fields($ds);
        $eq = ['ID_CLIENT','NAME','AGE','LAST_MODIFY','SUM','UUID'];
        self::assertSame($fields,$eq);
        // ------------------------------
        $q = 'select ID_CLIENT oid,NAME oName from '.TABLE_FILL;
        $ds = Base::ds($q,'test');
        $out = Base::fields($ds,false);
        $fields = [];
        foreach($out as $obj)
            $fields[]=(array)$obj;
        $eq = [ 
            [
                "name"=>"oid",
                "orgname"=>"ID_CLIENT",
                "table"=>"test_clients",
                "orgtable"=>"test_clients",
                "def"=>"",
                "db"=>"_wd_test",
                "catalog"=>"def",
                "max_length"=>2,
                "length"=>11,
                "charsetnr"=>63,
                "flags"=>49667,
                "type"=>3,
                "decimals"=>0,
                "stype"=>"int",
            ],[
                "name"=>"oName",
                "orgname"=>"NAME",
                "table"=>"test_clients",
                "orgtable"=>"test_clients",
                "def"=>"",
                "db"=>"_wd_test",
                "catalog"=>"def",
                "max_length"=>11,
                "length"=>768,
                "charsetnr"=>33,
                "flags"=>4097,
                "type"=>253,
                "decimals"=>0,
                "stype"=>"string",
            ]
        ];
        self::assertSame($fields,$eq);
        // ------------------------------
        $q = 'select ID_CLIENT oid,NAME oName from '.TABLE_FILL;
        $ds = Base::ds($q,'test');
        $fields = Base::fields($ds,['name','table']);
        $eq = [ 
            [
                "name"=>"oid",
                "table"=>"test_clients",
            ],[
                "name"=>"oName",
                "table"=>"test_clients",
            ]
        ];
        self::assertSame($fields,$eq);
        // ------------------------------
    }

    /**
     * @depends test_connect
     */    
    public function test_charSet(){
        // ------------------------------
        // charset is not set
        //$cs = Base::charSet('test');
        //self::assertSame($cs,'');
        // ------------------------------
        // first set charset (cant restory)
        Base::charSet('test','utf8');
        $cs = Base::charSet('test');
        self::assertSame($cs,'utf8');
        // ------------------------------
    }
    /**
     * @depends test_connect
     */    
    public function test_ds(){
        // ------------------------------
        $q = 'select * from '.TABLE_FILL;
        $ds = Base::ds($q,'test','utf8');
        self::assertTrue( Base::assign($ds) );
        // ------------------------------
        $q = 'select * from '.TABLE_FILL;
        $ds = Base::ds($q,'test');
        self::assertTrue( Base::assign($ds) );
        // ------------------------------
        self::assertTrue( !Base::isEmpty($ds) );
        // ------------------------------
        $q = 'select * from '.TABLE_EMPTY;
        $ds = Base::ds($q,'test');
        self::assertTrue( Base::isEmpty($ds) );
        // ------------------------------
        $q = 'select * from qwedwqd'.TABLE_FILL;
        $this->expectException(BaseException::class);        
        $ds = Base::ds($q,'test');
    }

    /**
     * @depends test_connect
     */    
    public function test_row(){

        $q = 'select * from '.TABLE_FILL;
        $ds = Base::ds($q,'test');
        $row = Base::row($ds);
        self::assertTrue(gettype($row) === 'array'  );

        $q = 'select * from '.TABLE_EMPTY;
        $ds = Base::ds($q,'test');
        $row = Base::row($ds);
        //error_log(gettype($row));
        self::assertNull($row);
    }

    /**
     * @depends test_connect
     */    
    public function test_read(){

        $q = 'select * from '.TABLE_FILL;
        $ds = Base::ds($q,'test');
        $count = Base::count($ds);
        $res = true;
        while($row = Base::read($ds)){
            if (gettype($row) !== 'array')
                $res = false;
            $count--;
        }
        self::assertTrue(($res&&$count === 0));


        $q = 'select * from '.TABLE_EMPTY;
        $ds = Base::ds($q,'test');
        $count = Base::count($ds);
        $res = true;
        while($row = Base::read($ds)){
            if (gettype($row) !== 'array')
                $res = false;
            $count--;
        }
        self::assertTrue(($res&&$count === 0));

    }

    /**
     * @depends test_connect
     */    
    public function test_value(){

        $q = 'select NAME from '.TABLE_FILL;
        $value = Base::value($q,'test');
        self::assertTrue($value === 'bbbb');
        // --------------------------------------------
        $q = 'select NAMEZ from '.TABLE_FILL;
        $value = Base::value($q,'test',['default'=>133]);
        self::assertTrue($value === 133);
        // --------------------------------------------
        $q = 'select NAME,AGE from '.TABLE_FILL;
        $value = Base::value($q,'test',['field'=>'AGE']);
        self::assertTrue($value==2);
        // --------------------------------------------
        $q = 'select NAMEZ from '.TABLE_FILL;
        $this->expectException(BaseException::class);
        $value = Base::value($q,'test');
        // --------------------------------------------
    }

    /**
     * @depends test_connect
     */    
    public function test_generate(){
        $table = 'MY_TABLE';
        $data = ['NAME'=>'Mike','AGE'=>12,'STORY'=>'ok','ID'=>102934];
        $types = ['NAME'=>'string','AGE'=>'float','STORY'=>'string'];
        $param = ['types'=>$types];
        // ----------------------------------------        
        $result = Base::generate('insert',$table,$data,$param);
        $equal = 'insert into `MY_TABLE` (`NAME`,`AGE`,`STORY`,`ID`) values ("Mike",12,"ok",102934)';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        
        $dataSelf = $data;
        $dataSelf['NAME']= [$dataSelf['NAME'],'string'];
        $dataSelf['STORY']= [$dataSelf['STORY'],'string'];
        $result = Base::generate('insert',$table,$dataSelf,[]);
        $equal = 'insert into `MY_TABLE` (`NAME`,`AGE`,`STORY`,`ID`) values ("Mike",12,"ok",102934)';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        
        $result = Base::generate('insert',$table,$data,array_merge($param,['exclude'=>'ID']));
        $equal = 'insert into `MY_TABLE` (`NAME`,`AGE`,`STORY`) values ("Mike",12,"ok")';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        
        $result = Base::generate('update',$table,$data,array_merge($param,['exclude'=>'ID']));
        $equal = 'update `MY_TABLE` set `NAME`="Mike",`AGE`=12,`STORY`="ok"';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        
        $result = Base::generate('update',$table,$data,array_merge($param,['include'=>'NAME,AGE']));
        $equal = 'update `MY_TABLE` set `NAME`="Mike",`AGE`=12';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        
        $result = Base::generate('insertOnDuplicate',$table,$data,array_merge($param,['exclude'=>'ID']));
        $equal = 'insert into `MY_TABLE` (`NAME`,`AGE`,`STORY`) values ("Mike",12,"ok") on duplicate key update `NAME`="Mike",`AGE`=12,`STORY`="ok"';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        

        $result = Base::generate('update',$table,$data,['where'=>'ID=::ID','types'=>$types]);
        $equal = 'update `MY_TABLE` set `NAME`="Mike",`AGE`=12,`STORY`="ok",`ID`=102934 where ID=102934';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        
        $result = Base::generate('update',$table,$data,['where'=>'ID=::ID and AGE=::AGE','exclude'=>'NAME','types'=>$types]);
        $equal = 'update `MY_TABLE` set `AGE`=12,`STORY`="ok",`ID`=102934 where ID=102934 and AGE=12';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        
        $result = Base::generate(
            'update',
            'DEALER',
            [
                'ID_DEALER' =>  1839,
                'NAME'      =>  ['Mike','string'],
                'DAY'       =>  10,
                'ARCH'      =>  [1,'string'],
            ],
            [
                'where' =>'ID_DEALER=::ID_DEALER or ARCH="1"',
                'exclude'=>['ID_DEALER','ARCH']
            ]
        );

        $equal = 'update `DEALER` set `NAME`="Mike",`DAY`=10 where ID_DEALER=1839 or ARCH="1"';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        
        // ----------------------------------------        
        $result = Base::generate(
            'update',
            'DEALER',
            [
                'ID_DEALER' =>  1839,
                'NAME'      =>  ['Mike','string'],
                'DAY'       =>  10,
                'ARCH'      =>  [1,'string'],
            ],
            [
                'rename'=>['ID_DEALER'=>'ID'],    
                'where' =>'ID=::ID or ARCH="1"',
                'exclude'=>['ID','ARCH']
            ]
        );

        $equal = 'update `DEALER` set `NAME`="Mike",`DAY`=10 where ID=1839 or ARCH="1"';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        


    }

    /**
     * @depends test_haveKeys
     */    
    public function test_paramToSql(){
        // ----------------------------------------        
        $q = 'select * from TEST where ID=:ID and NAME=":NAME"';
        $result = Base::paramToSql($q,['ID'=>1,'NAME'=>'mike']);
        $equal = 'select * from TEST where ID=1 and NAME="mike"';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        
        $q = 'select * from TEST where ID=:ID and NAME=:NAME';
        $result = Base::paramToSql($q,['ID'=>1,'NAME'=>['mike','string']]);
        $equal = 'select * from TEST where ID=1 and NAME="mike"';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        
        $q = 'select * from TEST where ID=:ID and NAME=:NAME';
        $result = Base::paramToSql($q,['ID'=>1,'NAME'=>'mike'],['types'=>['NAME'=>'string']]);
        $equal = 'select * from TEST where ID=1 and NAME="mike"';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        
        $q = 'select * from TEST where ID1=:ID and ID2=:ID2';
        $result = Base::paramToSql($q,['ID'=>'1','ID2'=>'2']);
        $equal = 'select * from TEST where ID1=1 and ID2=2';
        //error_log($result);
        self::assertSame($result,$equal);
        // ----------------------------------------        
    }

    /**
     * @depends test_connect
     */    
    public function test_update(){

        // --------------------------------------------
        $res = Base::update('test', TABLE_FILL , ['NAME'=>200,'AGE'=>100,'bron'=>333] ,'ID_CLIENT=33');
        self::assertTrue( $res );
        // --------------------------------------------
        $res = Base::update('test', TABLE_FILL , ['NAME'=>200,'AGE'=>100] ,'ID_CLIENT=32');
        self::assertTrue( $res );
        // --------------------------------------------
        $this->expectException(\Exception::class);
        $res = Base::update('test', TABLE_FILL , ['NAME'=>200,'AGE'=>100] ,'ID_CLIENTS=33');
        // --------------------------------------------
        
    }

    public function test_haveKeys(){
        $className = 'fmihel\base\Base';
        $method = '_haveKeys';

        $str = 'select * from table where ID = :ID';
        $keys = ['ID'];
        self::assertTrue( self::doPrivateStaticMethod($className,$method,$str,$keys) );
        // --------------------------------------------
        $str = 'select * from table where ID = :ID_K_MOD';
        $keys = ['ID'];
        $this->expectException(\Exception::class);
        self::assertTrue( self::doPrivateStaticMethod($className,$method,$str,$keys) );
        // --------------------------------------------
        $str = 'select * from table where ID = :ID_K_MOD';
        $keys = ['ID_K_MOD'];
        $this->expectException(\Exception::class);
        self::assertTrue( self::doPrivateStaticMethod($className,$method,$str,$keys) );

    }

    public function test_preparing(){

        // --------------------------------------------
        $q = 'select * from TEST where ID_TEST=?ID_TEST and NAME=?NAME';
        $fv = ['ID_TEST'=>'1.3','NAME'=>'Mike'];
        $format = ['ID_TEST'=>'float','NAME'=>'int'];
        $res = Base::preparing($q,$fv,$format);
        self::assertTrue($res['sql'] === 'select * from TEST where ID_TEST=? and NAME=?');
        self::assertTrue($res['format'] === 'di');
        self::assertTrue($res['values'][0] == '1.3');
        self::assertTrue($res['values'][1] == 'Mike');

        // --------------------------------------------
        $q = 'select * from TEST where ID_TEST=?ID_TEST and NAME=?NAME';
        $fv = ['ID_TEST'=>1.3,'NAME'=>'Mike'];
        $format = ['ID_TEST'=>'s'];
        $res = Base::preparing($q,$fv,$format);
        self::assertTrue($res['sql'] === 'select * from TEST where ID_TEST=? and NAME=?');
        self::assertTrue($res['format'] === 'ss');
        self::assertTrue($res['values'][0] == '1.3');
        self::assertTrue($res['values'][1] == 'Mike');
        // --------------------------------------------
        $q = 'select * from TEST where ID_TEST=?ID_TEST and NAME=?NAME';
        $fv = ['ID_TEST'=>[1,'int'],'NAME'=>'Mike'];
        $res = Base::preparing($q,$fv);
        self::assertTrue($res['sql'] === 'select * from TEST where ID_TEST=? and NAME=?');
        self::assertTrue($res['format'] === 'is');
        self::assertTrue($res['values'][0] == '1');
        self::assertTrue($res['values'][1] == 'Mike');
        // --------------------------------------------            
        $q = 'select * from TEST where ID_TEST=?ID_TEST and NAME=?NAME';
        $fv = ['ID_TEST'=>1,'NAME'=>'Mike'];
        $res = Base::preparing($q,$fv);
        self::assertTrue($res['sql'] === 'select * from TEST where ID_TEST=? and NAME=?');
        self::assertTrue($res['format'] === 'is');
        self::assertTrue($res['values'][0] == '1');
        self::assertTrue($res['values'][1] == 'Mike');
        // --------------------------------------------            
        $q = 'select * from TEST where ID_TEST=?ID_TEST and NAME=?NAME';
        $fv = ['ID_TEST'=>'1','NAME'=>'Mike'];
        $res = Base::preparing($q,$fv);
        self::assertTrue($res['sql'] === 'select * from TEST where ID_TEST=? and NAME=?');
        self::assertTrue($res['format'] === 'ss');
        self::assertTrue($res['values'][0] == '1');
        self::assertTrue($res['values'][1] == 'Mike');
        // --------------------------------------------            
        $q = 'select * from TEST where ID_TEST=?ID_TEST and NAME=?NAME';
        $fv = ['ID_TEST'=>1.3,'NAME'=>'Mike'];
        $res = Base::preparing($q,$fv);
        self::assertTrue($res['sql'] === 'select * from TEST where ID_TEST=? and NAME=?');
        self::assertTrue($res['format'] === 'ds');
        self::assertTrue($res['values'][0] == '1.3');
        self::assertTrue($res['values'][1] == 'Mike');
        // --------------------------------------------            
        $q = 'select * from TEST where ID_TEST=?ID_TEST and NAME=?NAME';
        $fv = ['ID_TEST'=>[1.3,'s'],'NAME'=>'Mike'];
        $res = Base::preparing($q,$fv);
        self::assertTrue($res['sql'] === 'select * from TEST where ID_TEST=? and NAME=?');
        self::assertTrue($res['format'] === 'ss');
        self::assertTrue($res['values'][0] == '1.3');
        self::assertTrue($res['values'][1] == 'Mike');
        // --------------------------------------------            
    }
     /**
     * @depends test_connect
     */    
    public function test_execute(){

        // --------------------------------------------
        $prep = Base::preparing(
            'update '.TABLE_FILL.' set NAME=?NAME,AGE=?AGE where ID_CLIENT=?ID_CLIENT',
            ['ID_CLIENT'=>2,'NAME'=>'Mike','AGE'=>['22','i']]
        );
        $res = Base::execute($prep,'test');
        self::assertTrue( true );
        // --------------------------------------------
        $prep = Base::preparing(
            'update '.TABLE_FILL.' set NAME=?NAME,AGE=?AGE where ID_CLIENT=?ID_CLIENT',
            ['ID_CLIENT'=>2,'NAME'=>'Mike','AGE'=>33],
            ['ID_CLIENT'=>'i','AGE'=>'integer','NAME'=>'s']
        );
        Base::startTransaction('test');
        $res = Base::execute($prep,'test');
        Base::rollback('test');
        self::assertTrue( true );
        // --------------------------------------------
        $prep = Base::preparing(
            'update '.TABLE_FILL.' set NAME=?NAME,AGE=?AGE where ID_CLIENT=?ID_CLIENT',
            ['ID_CLIENT'=>'3','NAME'=>'Nomad','AGE'=>'33333'],
            ['AGE'=>'i','ID_CLIENT'=>'i']
        );
        $res = Base::execute($prep,'test');
        self::assertTrue( true );
        // --------------------------------------------
    }
    protected static function doPrivateStaticMethod($className,$name,...$args) {
        $class = new \ReflectionClass($className);
        $method = $class->getMethod($name);
        $method->setAccessible(true);
        return $method->invoke(null,...$args);
      }

}

?>