<?php
namespace fmihel\base;

class DB
{
    public $db = null;
    public $transaction = 0;
    public $charset = '';
    public $errors = [];
    public $error = "";
    public $alias = "";
    public $baseName = '';

    function __construct($db, string $alias, string $baseName)
    {
        $this->db = $db;
        $this->alias = $alias;
        $this->baseName = $baseName;
    }
};
