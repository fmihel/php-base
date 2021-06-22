<?php
namespace fmihel\base;

class BaseException extends \Exception{
    function __construct($msg,$base=null)
    {
        $error = Base::error($base);
        if ($error)
            $msg = "{mysqli.error:$error} ".$msg;
        parent::__construct($msg,0);
    }
}

?>