<?php
namespace fmihel\base;

class _db{
    public $db;
    public $transaction;
    public $charset;
    public $errors=array();
    public $error="";
    function __construct(){
        $this->db=null;
        $this->transaction  = 0;
        $this->charset = '';
    }
};


?>
