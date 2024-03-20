<?php
namespace fmihel\base;

class Base
{

    private static $_base = [];
    private static $_codings = [];
    private static $_types = [];
    private static $_fieldsInfo = [];
    private static $stat_enable = false;
    private static $stat = [
        'count' => [],
    ];
    /** connect to base
     * @param string | array $server server name or ['server'=>...,'user'=>...,...]
     * @param string $user -   user name
     * @param string $pass -  password
     * @param string $baseName - base name
     * @param string $alias - alias for base
     * @param bool $die - if true then exit from script if connect error
     * @return bool
     */
    public static function connect($server, $user = '', $pass = '', $baseName = '', $alias = '', $die = true)
    {

        if (gettype($server) === 'array') {

            $p = array_merge([
                'server' => '',
                'user' => '',
                'pass' => '',
                'base' => '',
                'alias' => '',
                'die' => true,
            ], $server);

            $server = $p['server'];
            $user = $p['user'];
            $pass = $p['pass'];
            $baseName = $p['baseName'];
            $alias = $p['alias'];
            $die = $p['die'];

        };

        if (($server !== '') && ($user === '') && ($pass === '') && ($baseName === '') && ($alias === '')) {
            return isset(self::$_base[$server]) ? true : false;
        }

        if (isset(self::$_base[$alias])) {
            return true;
        }

        $db = new \mysqli($server, $user, $pass, $baseName);

        if ($db->connect_errno) {
            $msg = "can`t connect to MySQL: (" . $db->connect_errno . ") " . $db->connect_error;
            if ($die === 'exception') {
                throw new BaseException($msg);
            } elseif ($die) {
                echo $msg;
                exit;
            }

            return false;
        }

        $_db = new _db();
        $_db->db = $db;
        $_db->alias = $alias;

        self::$_base[$alias] = $_db;

        return true;

    }
    /** disconnect from base */
    public static function disconnect($base)
    {

        if (isset(self::$_base[$base])) {

            unset($base);
        }

    }
    /** set or return charset
     *
     * example set default charset
     * base::charSet('mybase','UTF-8');
     *
     * example return default charset
     * $v = base::charSet('mybase');
     *
     * example: story/restory codings
     * base::charSet('mybase','story');
     * base::charSet('mybase','UTF-8');
     * ...
     * base::charSet('mybase','restory');
     *
     */
    public static function charSet($base = null, $coding = null)
    {

        if ((gettype($coding) === 'string')) {
            if ($coding === '') {
                $coding = null;
            }

        } else {
            $coding = null;
        }

        // убираем путаницу с UTF-8 и utf8
        if (!is_null($coding)) {
            $coding = strtolower($coding);
            if ($coding === 'utf-8') {
                $coding = 'utf8';
            }

        }

        $_base = self::getbase($base);

        if (is_null($coding)) {
            return $_base->charset;
        } else {

            if ($coding === 'story') {

                if (!isset(self::$_codings[$base])) {
                    self::$_codings[$base] = [];
                }

                self::$_codings[$base][] = $_base->db->get_charset()->charset;

            } else if ($coding === 'restory') {
                $_base->db->set_charset(array_pop(self::$_codings[$base]));
            } else if (!$_base->db->set_charset($coding)) {
                throw new BaseException('error set charSet = ' . $coding, $base);
            } else {
                $_base->charset = $coding;
            }

        }
    }
    /**  return base or raise Exception */
    private static function getbase($base, $exception = true)
    {

        if (count(self::$_base) === 0) {
            if ($exception) {
                throw new BaseException("no have initializing base = $base");
            }

            return false;
        }

        $keys = array_keys(self::$_base);
        if (is_null($base)) {
            $base = $keys[0];
        }

        if (isset(self::$_base[$base])) {
            return self::$_base[$base];
        }

        if ($exception) {
            throw new BaseException("base $base is not exists");
        }

        return false;

    }
    /** return db object as ref to base */
    private static function db($base)
    {
        return self::getbase($base)->db;
    }
    /** return error of base */
    public static function error($base)
    {
        $_base = self::getbase($base);
        if ($_base) {
            return $_base->db->error;
        }

        return '';
    }

    public static function query($sql, $base, $coding = null)
    {

        $db = self::db($base);

        try {
            $change_coding = false;
            if (!is_null($coding)) {
                $story = self::charSet($base);
                self::charSet($base, $coding);
                $change_coding = true;
            }

            if (self::$stat_enable) {
                self::stat_query_count($sql);
            };

            $res = $db->query($sql);

            if ($res === false) {
                throw new BaseException($sql, $base);
            }

        } catch (\Exception $e) {
            if ($change_coding && (!is_null($coding))) {
                self::charSet($base, $story);
            }

            throw $e;
        };

        return $res;

    }

    /**
     * @return  false - если ошибка
     * @return object - если запрос выполнен
     */
    public static function ds($sql, $base, $coding = null)
    {

        $ds = self::query($sql, $base, $coding);
        $ds->data_seek(0);
        return $ds;
    }
    /**
     *
     */
    public static function assign($ds)
    {
        return (gettype($ds) === 'object');
    }
    public static function isEmpty($ds)
    {
        return ((!self::assign($ds)) || ($ds->num_rows === 0));
    }
    /** Возвращает кол-во записей запроса sql или в ds
     * если задать countFieldName, то будет искать соответсвтвующее поле и выдаст его значение
     * можно задать countFieldName как число, тогда это будет номер необходимого поля
     */
    public static function count($sqlOrDs, $base = null, $countFieldName = '')
    {

        $ds = gettype($sqlOrDs) === 'string' ? self::ds($sqlOrDs, $base, null) : $sqlOrDs;
        if (!$ds) {
            throw new BaseException('can`t get ds from sqlOrDs =  ' . print_r($sqlOrDs, true), $base);
        }

        $type = gettype($countFieldName);

        if (($countFieldName != '') || ($type === 'integer')) {
            $fields = self::fields($ds);

            if ($type === 'integer') {
                $row = self::row($ds);
                return intval($row[$fields[$countFieldName]]);
            } else {
                $fields = self::fields($ds);
                $countFieldName = strtoupper(trim($countFieldName));
                foreach ($fields as $name) {
                    if (
                        (strtoupper(trim($name)) === $countFieldName)
                        ||
                        (strpos(strtoupper(trim($name)), $countFieldName . '(') === 0)
                    ) {
                        $row = self::row($ds);
                        return intval($row[$name]);
                    }
                }
            }
        }

        return $ds->num_rows;
    }

    /** список таблиц */
    public static function tables($base)
    {
        $res = [];

        $q = 'SHOW TABLES';
        $ds = self::ds($q, $base);
        while ($row = self::read($ds)) {
            foreach ($row as $field => $table) {
                array_push($res, $table);
            }

        };

        return $res;

    }
    public static function haveField($field, $tableName, $base = null)
    {
        $list = self::fieldsInfo($tableName, $base, true);
        return (array_search($field, $list) !== false);
    }
    /** сокращенное имя типа возвращаемое по SHOW COLUMNS FROM...*/
    private static function shorType($type)
    {
        $match = [
            'int' => 'int',
            'int(' => 'int',
            'varchar' => 'string',
            'text' => 'string',
            'mediumtext' => 'string',
            'longtext' => 'string',
            'text' => 'string',
            'float' => 'float',
            'decimal' => 'float',
            'double' => 'float',
            'datetime' => 'date',
            'timestamp' => 'date',
            'blob' => 'blob',
            'longblob' => 'blob',
            'mediumblob' => 'blob',
        ];
        foreach ($match as $m => $ret) {
            if (strpos($type, $m) === 0) {
                return $ret;
            }

        }
        return 'uncknown';
    }
    /** возвращает либо список имен полей
     *  short = true | 'short'  - список полей
     *  short = 'types' список [поле=>тип,...]
     *  short = false|'full' полную информацию [ [Fiel=>'name',Type=>'string',...], ..] ]
     */
    public static function fieldsInfo($tableName, $base = null, $short = true, $refresh = false)
    {

        $shortIndex = 'short';
        if ($short === false || $short === 'full') {
            $shortIndex = 'full';
        }

        if ($short === 'types') {
            $shortIndex = 'types';
        }

        $db = self::getbase($base);
        $base = $db->alias;

        if (!$refresh && isset(self::$_fieldsInfo[$base][$tableName][$shortIndex])) {
            return self::$_fieldsInfo[$base][$tableName][$shortIndex];
        }

        $out = [];

        $q = 'SHOW COLUMNS FROM `' . $tableName . '`';
        $ds = self::ds($q, $base);

        while ($row = self::read($ds)) {

            if (($short === true) || ($short === 'short')) {
                $out[] = $row['Field'];
            } elseif ($short === 'types') {
                $out[$row['Field']] = self::shorType($row['Type']);
            } else {
                $out[] = $row;
            }

        }

        if (!isset(self::$_fieldsInfo[$base])) {
            self::$_fieldsInfo[$base] = [$tableName => [$shortIndex => null]];
        }

        if (!isset(self::$_fieldsInfo[$base][$tableName])) {
            self::$_fieldsInfo[$base][$tableName] = [$shortIndex => null];
        }

        self::$_fieldsInfo[$base][$tableName][$shortIndex] = $out;
        return $out;

    }

    /** возвращает массив с типами для каждого поля таблицы $table базы $base, для использования в generate или update
     *  данные кешируются, поэтому при изменении структуры таблицы необходимо вызвать getTypes c $refresh =  true
     */
    public static function getTypes(string $table, string $base, bool $refresh = false)
    {

        if (!isset(self::$_types[$base])) {
            self::$_types[$base] = [];
        }

        if ($refresh || !isset(self::$_types[$base][$table])) {
            self::$_types[$base][$table] = self::fieldsInfo($table, $base, 'types');
        }

        return self::$_types[$base][$table];
    }
    /** возвращает информацию о полях результата запроса
     * @param object $ds - ссылка на результат запроса
     * @param bool | array $short_info true-короткая информация, false полная информация ,[field1,field2,..] - только выборочные поля из запроса
     * @return array | object
     */
    public static function fields($ds, $short_info = true)
    {
        $ff = $ds->fetch_fields();
        if (gettype($short_info) === 'array') {
            $out = [];
            for ($i = 0; $i < count($ff); $i++) {
                $item = (array) $ff[$i];
                $dat = [];
                foreach ($short_info as $info) {
                    if (isset($item[$info])) {
                        $dat[$info] = $item[$info];
                    }

                }
                $out[] = $dat;
            }
            return $out;
        } elseif ($short_info) {
            $out = [];
            for ($i = 0; $i < count($ff); $i++) {
                $out[] = $ff[$i]->name;
            }

            return $out;

        } else {

            for ($i = 0; $i < count($ff); $i++) {
                $ff[$i]->stype = self::fieldTypeToStr($ff[$i]->type);
            }

            return $ff;
        }

    }
    /**  from: http://php.net/manual/ru/mysqli-result.fetch-fields.php */
    public static function map_field_type_to_bind_type($field_type)
    {
        switch ($field_type) {
            case MYSQLI_TYPE_DECIMAL:
            case MYSQLI_TYPE_NEWDECIMAL:
            case MYSQLI_TYPE_FLOAT:
            case MYSQLI_TYPE_DOUBLE:
                return 'd';

            case MYSQLI_TYPE_BIT:
            case MYSQLI_TYPE_TINY:
            case MYSQLI_TYPE_SHORT:
            case MYSQLI_TYPE_LONG:
            case MYSQLI_TYPE_LONGLONG:
            case MYSQLI_TYPE_INT24:
            case MYSQLI_TYPE_YEAR:
            case MYSQLI_TYPE_ENUM:
                return 'i';

            case MYSQLI_TYPE_TIMESTAMP:
            case MYSQLI_TYPE_DATE:
            case MYSQLI_TYPE_TIME:
            case MYSQLI_TYPE_DATETIME:
            case MYSQLI_TYPE_NEWDATE:
            case MYSQLI_TYPE_INTERVAL:
            case MYSQLI_TYPE_SET:
            case MYSQLI_TYPE_VAR_STRING:
            case MYSQLI_TYPE_STRING:
            case MYSQLI_TYPE_CHAR:
            case MYSQLI_TYPE_GEOMETRY:
                return 's';

            case MYSQLI_TYPE_TINY_BLOB:
            case MYSQLI_TYPE_MEDIUM_BLOB:
            case MYSQLI_TYPE_LONG_BLOB:
            case MYSQLI_TYPE_BLOB:
                return 'b';

            default:
                trigger_error("unknown type: $field_type");
                return 's';
        };
    }

    public static function fieldTypeToStr($field_type)
    {

        switch ($field_type) {

            case MYSQLI_TYPE_DECIMAL:
            case MYSQLI_TYPE_NEWDECIMAL:
            case MYSQLI_TYPE_FLOAT:
            case MYSQLI_TYPE_DOUBLE:
                return 'float';

            case MYSQLI_TYPE_BIT:
            case MYSQLI_TYPE_TINY:
            case MYSQLI_TYPE_SHORT:
            case MYSQLI_TYPE_LONG:
            case MYSQLI_TYPE_LONGLONG:
            case MYSQLI_TYPE_INT24:
            case MYSQLI_TYPE_YEAR:
            case MYSQLI_TYPE_ENUM:
                return 'int';

            case MYSQLI_TYPE_TIMESTAMP:
            case MYSQLI_TYPE_DATE:
            case MYSQLI_TYPE_TIME:
            case MYSQLI_TYPE_DATETIME:
            case MYSQLI_TYPE_NEWDATE:
            case MYSQLI_TYPE_INTERVAL:
                return 'date';

            case MYSQLI_TYPE_SET:
            case MYSQLI_TYPE_VAR_STRING:
            case MYSQLI_TYPE_STRING:
            case MYSQLI_TYPE_CHAR:
            case MYSQLI_TYPE_GEOMETRY:
                return 'string';

            case MYSQLI_TYPE_TINY_BLOB:
            case MYSQLI_TYPE_MEDIUM_BLOB:
            case MYSQLI_TYPE_LONG_BLOB:
            case MYSQLI_TYPE_BLOB:
                return 'blob';

            default:
                return 'uncknown';
        };
    }

    /** перемещает указатель на первую запись */
    public static function first($ds)
    {
        $ds->data_seek(0);
    }
    /** возвращает текущую строку
     * если строки закончились или их нет, то возвращает NULL
     */
    public static function row($sqlOrDs, $base = null, $coding = null)
    {

        $ds = gettype($sqlOrDs) === 'string' ? self::ds($sqlOrDs, $base, $coding) : $sqlOrDs;

        if (self::isEmpty($ds)) {
            return null;
        }

        return $ds->fetch_assoc();
    }
    /** возвращает список строк запроса,
     * @param {string || dataset} запрос или датасет
     * @param {string} алиас базы
     * @param {string} кодировка
     * @param {function} ф-ция вызываемая для каждой строки списка, должна вернуть измененную строку либо false, тогда строки не будет в выходном списке
     * Ex:
     *  $rows = Base::rows('select ID_CLIENT,AGE,SUM from CLIENTS','base','utf8',function($row,$i){
     *              if ($row['AGE']>20) // только старше 20
     *                  return false;
     *              $row['SUM'] += 10; // изменяем поле
     *              $row['NEW_FIELD'] = rand(100,200); // добавляем новое поле
     *              return $row;
     *         });
     */
    public static function rows($sqlOrDs, $base = null, $coding = null, $filter = false)
    {
        $out = [];
        $ds = gettype($sqlOrDs) === 'string' ? self::ds($sqlOrDs, $base, $coding) : $sqlOrDs;
        $row_num = 0;
        while ($row = self::read($ds)) {

            if ($filter) {
                $row = $filter($row, $row_num);
                $row_num += 1;
            }
            if ($row) {
                $out[] = $row;
            }

        }
        return $out;
    }

    /** используется для чтения строки из dataset
     * Ex:
     * $ds = base::ds(...)l
     * while($row=base::read($ds)){
     *    ....
     * }
     * @param {dataset} $ds - результат base::ds()
     * @return array(..) | NULL
     */
    public static function read($ds)
    {
        return $ds->fetch_assoc();
    }

    public static function value($sql, $base, $param = [])
    {
        $p = array_merge([
            'field' => '',
            'default' => null,
            'coding' => null,
            'limit' => true,
        ], $param);

        $field = $p['field'];
        $default = $p['default'];
        $coding = $p['coding'];

        try {

            if ($p['limit'] === true) {
                if (preg_match('/\s+limit\s+[0-9]+[\s\S]*\Z/m', $sql) !== 1) {
                    $sql .= ' limit 1';
                }

            }

            $ds = self::ds($sql, $base, $coding);

            $fields = self::fields($ds);

            if ($field === '') {
                $field = $fields[0];
            } else if (array_search($field, $fields) === false) {
                throw new BaseException(" field = ['$field'] not exist", $base);
            }

            if (self::isEmpty($ds)) {
                throw new BaseException('result of [' . $sql . '] is empty', $base);
            }

            $row = self::row($ds);
            return $row[$field];

        } catch (\Exception $e) {
            if ($default === null) {
                throw $e;
            }
        };

        return $default;
    }

    public static function startTransaction($base)
    {
        $b = self::getbase($base);

        if ($b->transaction == 0) {
            //$b->db->autocommit(false);
            $b->db->begin_transaction();
        }
        $b->transaction += 1;
        return true;
    }

    public static function commit($base)
    {
        $b = self::getbase($base);

        $b->transaction -= 1;

        if ($b->transaction == 0) {
            $b->db->commit();
            return true;
        }

        if ($b->transaction < 0) {
            throw new BaseException('transaction overflow loop...', $base);
        }

        return false;
    }

    public static function rollback($base = null)
    {
        $b = self::getbase($base);

        $b->transaction -= 1;

        if ($b->transaction == 0) {
            $b->db->rollback();
            return true;
        }

        if ($b->transaction < 0) {
            throw new BaseException('transaction overflow loop...', $base);
        }

        return false;

    }

    private static function uuidProxy()
    {

        $chrLeft = 97; //a
        $chrRight = 102; //f
        $chr0 = 48;
        $chr9 = 57;

        $is_num = (rand(0, 10) < 7 ? true : false);

        if ($is_num) {
            $code = rand($chr0, $chr9);
        } else {
            $code = rand($chrLeft, $chrRight);
        }

        return chr($code);
    }

    public static function uuid($count = 32)
    {
        $uuid = '';
        for ($i = 0; $i < $count; $i++) {
            $uuid .= self::uuidProxy();
        }

        return $uuid;
    }

    public static function insert_uuid($table, $index, $base, $fieldUUID = 'UUID', $countUUID = 32)
    {
        $uuid = self::uuid($countUUID);

        $q = 'insert into `' . $table . '` set `' . $fieldUUID . "` = '" . $uuid . "'";
        self::query($q, $base);

        $q = 'select `' . $index . '` from `' . $table . '` where `' . $fieldUUID . "`='" . $uuid . "'";
        return self::value($q, $base, ['field' => $index]);

    }

    /** преобоазует значение к представлеию в SQL запросе в зависимости от его типа */
    public static function typePerform($value, $type)
    {

        if (($type === 'string') || ($type === 'date')) {
            return '"' . self::esc($value) . '"';
        }
        return $value;

    }
    /** генерация текста запроса по входным данным
     * @param {string} typeQuery insert|update|insertOnDuplicate
     * @param {string} table - имя таблицы
     * @param {array} data  =  [ FIELD_NAME=>VALUE , FIELD_NAME=>[VALUE] , FIELD_NAME=>[VALUE,TYPE],...]
     * Если указывать значение в скобках [VALUE] то его тип будет определяться автоматический
     * Если указать тип значения [VALUE,TYPE] то данный тип будет иметь приоритет над указанным в param->types
     * @param {array} param =
     *   types=>array,           - array('NAME'=>'string',..) для получения списка типов полей, можно воспользоваться base::fieldsInfo(base,'types');
     *   include=>array|string,
     *   exclude=>array|string
     *   rename=>array,
     *   where=>string  - выражение, которое (если есть) будет использоваться в запросе update
     *   refactoring = true - вывод в удобном для анализа виде
     *   alias=>array|string|string;string (префексы перед именем поля, перед формированием запроса он удаляется, преобразуя поле
     *                 в соотвествующее в таблице
     *   ex: alias = "tab";
     *       "tab_NAME" - > "NAME"
     *
     * @return string|bool    вернет либо запрос, либо false если ни одного поля не было добавлено в запрос
     */
    public static function generate($queryType, $table, $data, $param = [])
    {

        $types = isset($param['types']) ? $param['types'] : [];
        $bTypes = count($types) > 0;

        $exclude = isset($param['exclude']) ? $param['exclude'] : array();
        if (gettype($exclude) === 'string') {
            $exclude = explode(';', str_replace(',', ';', $exclude));
        }

        $bExclude = count($exclude) > 0;

        $include = isset($param['include']) ? $param['include'] : array();
        if (gettype($include) === 'string') {
            $include = explode(';', str_replace(',', ';', $include));
        }

        $bInclude = count($include) > 0;

        $rename = isset($param['rename']) ? $param['rename'] : array();
        $bRename = count($rename) > 0;

        $index = isset($param['index']) ? $param['index'] : '';
        if (($queryType === 'insertOnDuplicate') && ($index === '')) {
            foreach ($data as $f => $v) {
                if (mb_strpos(strtoupper($f), 'ID') !== 0) {
                    $index = $f;
                    break;
                }
            }
        }

        $where = trim(isset($param['where']) ? $param['where'] : '');
        if (($where !== '') && (mb_strpos(strtoupper($where), 'WHERE') !== 0)) {
            $where = 'where ' . $where;
        }

        $pref = isset($param['alias']) ? $param['alias'] : array();
        if (is_string($pref)) {
            $pref = array($pref);
        }

        $bPref = count($pref) > 0;

        if (isset($param['refactoring']) && $param['refactoring'] === true) {
            $bRef = true;
            $DCR = "\n\t";
            $CR = "\n";
        } else {
            $bRef = false;
            $DCR = '';
            $CR = "";
        }

        $insertBlockLeft = '';
        $insertBlockRight = '';
        $updateBlock = '';

        $is_empty = true;
        $whereHaveVar = strpos($where, '::') === false ? false : true;

        foreach ($data as $f => $v) {

            $need = true;
            $field = $f;
            $value = $v;
            $valType = gettype($value);

            if (($need) && ($bPref)) {
                for ($i = 0; $i < count($pref); $i++) {
                    if (strpos($field, $pref[$i] . '_') === 0) {
                        $field = str_replace($pref[$i] . '_', '', $field);
                        break;
                    }
                }
            }

            if (($need) && ($bRename)) {
                if (isset($rename[$field])) {
                    $field = $rename[$field];
                }

            }

            if ($bInclude) {
                $need = (array_search($field, $include) !== false);
            }

            if (($need) && ($bExclude)) {
                $need = (array_search($field, $exclude) === false);
            }

            if (($need) && ($bTypes || $valType === 'array')) {
                if ($valType === 'array') {
                    $tp = count($value) > 1 ? $value[1] : gettype($value[0]);
                    $value = self::typePerform($value[0], $tp);
                } elseif (isset($types[$field]) !== false) {
                    $tp = $types[$field];
                    $value = self::typePerform($value, $tp);
                }
            }

            if ($whereHaveVar) {
                $whereValue = $value;
                if (!(($need) && ($bTypes || $valType === 'array'))) {
                    if ($valType === 'array') {
                        $tp = count($value) > 1 ? $value[1] : gettype($value[0]);
                        $whereValue = self::typePerform($value[0], $tp);
                    } elseif (isset($types[$field]) !== false) {
                        $tp = $types[$field];
                        $whereValue = self::typePerform($value, $tp);
                    }
                }

                $where = str_replace('::' . $field, $whereValue, $where);
            }

            if ($need) {

                $tab = '';
                if ($bRef) {
                    $sl = strlen($field) + 2 + ($queryType !== 'insert' ? 1 : 0);
                    $tab = ($sl < 8 ? "\t\t" : ($sl < 17 ? "\t" : ""));
                };

                $updateBlock .= ($updateBlock !== '' ? ',' : '') . $DCR . "`$field`" . '=' . $tab . $value;
                $insertBlockLeft .= ($insertBlockLeft !== '' ? ',' : '') . $DCR . "`$field`";
                if (($queryType === 'insert') || ($queryType === 'insertOnDuplicate')) {
                    if ($bRef) {
                        $insertBlockLeft .= $tab . '/*' . $value . '*/';
                    }

                    $insertBlockRight .= ($insertBlockRight !== '' ? ',' : '') . $value;
                }
                $is_empty = false;
            }
        }

        if ($is_empty) {
            return false;
        } else {
            $result = false;
            if ($queryType === 'insert') {
                $result = 'insert into ' . $DCR . "`$table` " . $CR . "(" . $CR . "$insertBlockLeft" . $CR . ") " . $CR . "values ($insertBlockRight) ";
            } elseif ($queryType === 'update') {
                $result = 'update ' . $DCR . "`$table` " . $CR . "set $updateBlock " . $CR . $where . ' ';
            } elseif ($queryType === 'insertOnDuplicate') {
                $result = 'insert into ' . $DCR . "`$table` " . $CR . "(" . $CR . "$insertBlockLeft" . $CR . ") " . $CR . "values ($insertBlockRight) " . $CR . "on duplicate key update " . "$updateBlock ";
            }
            return trim($result);
        }
    }

    /** преобразуем данные из $param в список полей для формирования запроса sql */
    public static function fieldsToSQL($param)
    {

        $exclude = isset($param['exclude']) ? $param['exclude'] : array();
        if (gettype($exclude) === 'string') {
            $exclude = explode(';', str_replace(',', ';', $exclude));
        }

        $bExclude = count($exclude) > 0;

        $include = isset($param['include']) ? $param['include'] : array();
        if (gettype($include) === 'string') {
            $include = explode(';', str_replace(',', ';', $include));
        }

        $bInclude = count($include) > 0;

        $pref = isset($param['alias']) ? $param['alias'] : array();
        $bPref = count($pref) > 0;

        $types = isset($param['types']) ? $param['types'] : array();
        $bTypes = count($types) > 0;

        if ($param['refactoring'] === true) {
            $DCR = "\n\t";
            $CR = "\n";
        } else {
            $DCR = '';
            $CR = "";
        }
        $fields = array();
        if ($bTypes) {
            $_f = array_keys($types);
            //_LOG('['.print_r($include,true).']',__FILE__,__LINE__);

            for ($i = 0; $i < count($_f); $i++) {
                $need = true;
                $field = $_f[$i];
                //_LOG("$field",__FILE__,__LINE__);

                if ($bInclude) {
                    $need = (array_search($field, $include) !== false);
                }

                //_LOG("$field:[$need]",__FILE__,__LINE__);

                if (($need) && ($bExclude)) {
                    $need = (array_search($field, $exclude) === false);
                }

                if ($need) {
                    $fields[] = $field;
                }

            }
        } else if ($bInclude) {
            $_f = $include;
            for ($i = 0; $i < count($_f); $i++) {
                $need = true;
                $field = $_f[$i];

                if ($bExclude) {
                    $need = (array_search($field, $exclude) === false);
                }

                if ($need) {
                    $fields[] = $field;
                }

            }

        } else {
            return '';
        }

        //_LOG('['.print_r($fields,true).']',__FILE__,__LINE__);

        $res = '';

        for ($i = 0; $i < count($fields); $i++) {
            $res .= ($res === '' ? $CR : ',' . $CR) . $pref . '.' . $fields[$i] . ' ' . $pref . '_' . $fields[$i];
        }

        return $res . ' ' . $CR;
    }

    public static function real_escape($string, $base = null)
    {
        $db = self::db($base);
        if (!$db) {
            return $string;
        } else {
            return $db->real_escape_string($string);
        }

    }

    public static function esc($string)
    {
        $from = array('"');
        $to = array('\"');
        return str_replace($from, $to, $string);
    }
    /** заменяет параметры типа :FIELDNAME в $sql , на их значение value из $FeildNameValue=['FIELDNAME'=>value]
     *  тк же можно задать $value [VALUE,TYPE] либо указать в $param['types'=>]
     */
    public static function paramToSql(string $sql, array $FieldNameValue = [], array $param = [])
    {

        $param = array_merge([
            'types' => [],
        ], $param);

        $fields = array_keys($FieldNameValue);
        self::_haveKeys($sql, $fields);
        uksort($FieldNameValue, function ($a, $b) {return strlen($a) < strlen($b);});
        $types = $param['types'];
        $from = [];
        $to = [];
        foreach ($FieldNameValue as $name => $value) {
            $type = 'int';
            if (gettype($value) === 'array') {
                $type = $value[1];
                $value = $value[0];
            } elseif (isset($types[$name])) {
                $type = $types[$name];
            };

            $value = self::typePerform($value, $type);
            $from[] = ':' . $name;
            $to[] = $value;
        }
        return str_replace($from, $to, $sql);

    }
    /** внесение изменений в таблицу
     * @return {boolean || Exception}
     */
    public static function update(string $base, string $tableName, array $data, string $where, $coding = null)
    {

        $types = self::getTypes($tableName, $base);
        $include = array_keys($types);
        $q = self::generate('update', $tableName, $data, ['types' => $types, 'where' => $where, 'include' => $include]);
        Base::query($q, $base, $coding);
        return true;

    }
    /** проверка, что keys содержит все подставляемые переменные для $sql
     * Ex: sqlHavekeys('select * from one where id=:id and name=":name"',["id",'name','data']) = true
     * Ex: sqlHavekeys('select * from one where id=:id and name=":name"',["id",'data']) : raise Exception
     */
    private static function _haveKeys(string $sql, array $keys): bool
    {

        $re = '/(:)([a-zA-Z_0-9]+)/m';
        preg_match_all($re, $sql, $result);
        $result = array_unique($result[2]);
        foreach ($result as $name) {
            if (array_search($name, $keys) === false) {
                throw new BaseException("field $name not exists");
            }
        }

        return true;
    }

    /** Эквивален Oracle CONNECT BY PRIOR
     * Ф-ция работает не совсем как в Oracle, в Oracle сначала создаться полный список по связке  prior (по всему дереву),
     * а к выводу пойдут лишь те, что удовлктворяют внутреннему where
     * А данной ф-ции, если указать внутренний where то дерево может не быть построено до конца, так как нужные строки будут отфильтрованы
     * на этапе запроса. Так что лучше сначала построить дерево, а потом полученный список отфильтровать
     *
     * @param {string} - запрос без where
     * @param {string} - where для поиска первой строки
     * @param {string} - where для поиска дочерних элементов, для ссылки на родительский элемент использовать префикс prior.FIELD_NAME
     * @param {string} - алиас базы
     * @param {string} - кодировка
     *
     * Examples:
     * tabel NAMES
     * id own name   age
     * 1  0   mike   12
     * 2  1   aaa    12
     * 3  1   bbb    7
     * 4  2   ccc    12
     *
     * connect_by_prior('select * from NAMES n','n.id=1','n.age>10 and n.own=prior.id','deco','utf8');
     * >> 1  0   mike   12
     * >> 2  1   aaa    12
     * >> 4  2   ccc    12
     *
     */
    public static function connect_by_prior(string $sql, string $start, string $prior, string $base = null, $coding = 'utf8'): array
    {
        $out = [];
        // поиск первой строки
        $q = $sql . ' where ' . $start;
        $ds = self::ds($q, $base, $coding);

        if (!self::isEmpty($ds)) {

            $out[] = self::read($ds);

            // список полей
            $fields = self::fields($ds, true);
            usort($fields, function ($a, $b) {
                return strlen($a) < strlen($b);
            }); // уопрядочим по убыванию длины имени
            $priorFields = array_map(function ($item) {return 'prior.' . $item;}, $fields); // список полей к замене
            // шаблон для поиска след уровня
            $templ = $sql . ' where ' . $prior;

            $childs = self::_connect_by_prior($out[0], $templ, $fields, $priorFields, $base, $coding);
            $out = array_merge($out, $childs);
        }

        return $out;
    }
    private static function _connect_by_prior($current, $templ, $fields, $priorFields, $base, $coding): array
    {
        $out = [];
        $values = array_map(function ($name) use ($current) {return $current[$name];}, $fields);
        $q = str_replace($priorFields, $values, $templ);
        $ds = self::ds($q, $base, $coding);
        while ($row = self::read($ds)) {
            $out[] = $row;

            $childs = self::_connect_by_prior($row, $templ, $fields, $priorFields, $base, $coding);
            if (count($childs) > 0) {
                $out = array_merge($out, $childs);
            }

        }
        return $out;
    }

    public static function prepare($template, $base)
    {
        $db = self::db($base);

        if (self::$stat_enable) {
            self::stat_query_count($template);
        };

        $res = $db->prepare($template);
        if ($res === false) {
            throw new BaseException('mysqli::prepare=false (template:' . print_r($template) . ')');
        }

        return $res;
    }
    /** выполняет запрос по шаблону
     *
     *  Ex:
     *  $q = 'update TEST set NAME=?NAME where ID_TEST=?ID_TEST';
     *  $prep = Base::preparing('update TEST set NAME=?NAME where ID_TEST=?ID_TEST',['ID_TEST'=>1,'NAME'=>'Mike'])

     *  Base::execute($prep,'xxx','utf8');
     *
     */
    public static function execute(array $preparing, $base = null, $coding = null)
    {

        $charSet = self::charSet($base);
        self::charSet($base, $coding);
        try {
            if (isset($preparing['sql'])) {
                $prepare = self::prepare($preparing['sql'], $base);
            } else {
                $prepare = $preparing['prepare'];
            }

            $prepare->bind_param($preparing['format'], ...$preparing['values']);
            if (!$prepare->execute()) {
                throw new BaseException('mysqli::execute = false');
            }

            self::charSet($base, $charSet);
            return $prepare;

        } catch (\Exception $e) {

            self::charSet($base, $charSet);
            throw $e;
        };

    }

    private static function _prepFormat(string $vt)
    {
        if ($vt === 'string' || $vt === 's') {
            return 's';
        }

        if (($vt === 'int') || ($vt === 'integer') || ($vt === 'i')) {
            return 'i';
        }

        if (($vt === 'float') || ($vt === 'double') || ($vt === 'f') || ($vt === 'd')) {
            return 'd';
        }

        if (($vt === 'blob') || ($vt === 'b')) {
            return 'b';
        }

        return false;
    }
    /** Обработка шаблона запроса
     *  Ex1:
     *  $q = 'select * from TEST where ID_TEST = ?ID_TEST and NAME=?NAME';
     *  $fields = ['ID_TEST'=>1,'NAME'=>'Mike']
     *  $res = Base::preparing($q,$fields);
     *
     *  result:
     *  $res['sql']  -  'select * from TEST where ID_TEST=? and NAME=?'
     *  $res['format'] - 'is'
     *  $res['values] - [1,'Mike']
     *
     *  Ex2:
     *  $q = 'select * from TEST where ID_TEST = ?ID_TEST and NAME=?NAME';
     *  $fields = ['ID_TEST'=>[1,'int'],'NAME'=>'Mike']
     *  $res = Base::preparing($q,$fields);
     *
     *  result:
     *  $res['sql']  -  'select * from TEST where ID_TEST=? and NAME=?'
     *  $res['format'] - 'is'
     *  $res['values] - [1,'Mike']
     */
    public static function preparing(string $sql, array $FieldNameValue = [], $FieldFormat = []): array
    {
        $re = '/(\?[A-Za-z\_0-9]+)/m';
        preg_match_all($re, $sql, $matches, PREG_SET_ORDER, 0);
        $vars = [];
        foreach ($matches as $m) {
            $vars[] = $m[0];
        }
        // выстроить переменные в порядке убывания, для нормального срабатывания str_replace
        $to_replace = array_merge($vars);
        usort($to_replace, function ($a, $b) {return strlen($a) < strlen($b);});
        $sql = str_replace($to_replace, '?', $sql);

        $format = '';
        $values = [];
        foreach ($vars as $var) {
            $name = mb_substr($var, 1);
            if (!isset($FieldNameValue[$name])) {
                throw new \Exception(" not exists $name in FieldNameValue");
            }

            $value = $FieldNameValue[$name];

            $type = false;
            if (isset($FieldFormat[$name])) {
                $type = self::_prepFormat($FieldFormat[$name]);
            }

            if ($type === false) {
                if (gettype($value) === 'array') {
                    $vt = $value[1];
                    $type = self::_prepFormat($vt);
                    $value = $value[0];
                } else {
                    if (!is_string($value)) {
                        if (is_numeric($value)) {
                            if (strpos($value, '.') === false) {
                                $type = 'i';
                            } else {
                                $type = 'd';
                            }

                        }
                    }
                }
            }

            if ($type === false) {
                $type = 's';
            }

            $format .= $type;
            $values[] = $value;

        }

        return ['sql' => $sql, 'format' => $format, 'values' => $values];
    }
    /** клонирует строку таблицы
     * @param {string} tableName - имя таблицы
     * @param {string} where - условие в запросе SQL для выбора копируемой строки (Ex: `ID_ORDER = 10 and ARCH <> 1`)
     * @param {string} base - имя базы
     * @param {array} params - набор дополнительных параметров
     * @return {array} массив с клонируемыми данными ,если существует UUID в противном случае - []
     */
    public static function cloneRecord($tableName, $where, $base, $params = [])
    {
        $params = array_merge([
            'include' => [], // [field,field] // включаемые поля для копирования ( если ничего не задать беруться все)
            'exclude' => [], // [field,field] // исключаемые поля из списка включаемых
            'change' => [], //[field=>value,...] // поля, которые будут заменены
            'coding' => null, // кодировка
            'UUID-NAME' => 'UUID', // имя уникального поля для начальной идентификации
            'UUID-SIZE' => 32, // размер поля начальной идентификации
            'return' => '*', // список возвращаемых полей (будет возвращен, если существует поле UUID-NAME)
        ], $params);

        $types = self::fieldsInfo($tableName, $base, 'types');

        //$all = self::fieldsInfo($tableName, $base, true);
        $types = self::fieldsInfo($tableName, $base, 'types');
        $include = (gettype($params['include']) !== 'array' || count($params['include']) === 0) ? array_keys($types) : $params['include'];
        $exclude = (gettype($params['exclude']) !== 'array') ? [] : $params['exclude'];
        $fields = [];

        $have_uuid = (isset($types[$params['UUID-NAME']]) !== false && $params['UUID-NAME']);
        if ($have_uuid && array_search($params['UUID-NAME'], $include) === false) {
            $include[] = $params['UUID-NAME'];
        }

        foreach ($include as $field) {
            if (array_search($field, $exclude) === false) {
                $fields[] = '`' . $field . '`';
            }
        }

        $insert = implode(',', $fields);
        $select = $insert;

        if ($have_uuid) {
            $uuid = self::uuid($params['UUID-SIZE']);
            $select = str_replace('`' . $params['UUID-NAME'] . '`', '"' . $uuid . '"', $select);
        }

        if (!empty($params['change'])) {
            foreach ($params['change'] as $name => $val) {
                $val = self::typePerform($val, $types[$name]);
                $select = str_replace('`' . $name . '`', $val, $select);
            }
        }

        $q = 'insert into `' . $tableName . '` (' . $insert . ') select ' . $select . ' from ' . $tableName . ' where ' . $where;

        self::query($q, $base, $params['coding']);

        if ($have_uuid) {

            $re = $params['return'];
            $q = 'select ' . (gettype($re) === 'array' ? implode(',', $re) : $re) . ' from `' . $tableName . '` where `' . $params['UUID-NAME'] . '` = "' . $uuid . '"';

            return Base::row($q, $base, $params['coding']);

        }

        return [];
    }
    public static function exists($sql, $base): bool
    {
        if (strpos(strtoupper($sql), 'LIMIT ') === false) {
            $sql = $sql . ' limit 1';
        };
        $ds = self::ds($sql, $base);
        return $ds && $ds->num_rows > 0;
    }
    /** вкл/выкл ркежим записи статистики */
    public static function stat_enable($set = null): bool
    {
        if (!is_null($set)) {
            self::$stat_enable = !!$set;
        }
        return self::$stat_enable;
    }
    /** вкл/выкл ркежим записи статистики */
    private static function stat_count($msg)
    {
        if (self::$stat_enable) {
            if (isset(self::$stat['count'][$msg])) {
                self::$stat['count'][$msg] = ++self::$stat['count'][$msg];
            } else {
                self::$stat['count'][$msg] = 1;
            }
        }
    }
    private static function stat_query_count($query)
    {
        $upper = strtoupper(trim($query));
        $commands = ['SELECT', 'INSERT', 'UPDATE', 'CREATE', 'DROP', 'ALTER', 'SHOW', 'USE', 'SECRIBE', 'DELETE'];
        $max = -1;
        $msg = false;
        foreach ($commands as $com) {
            $pos = strpos($upper, $com);
            if ($pos !== false) {
                if ($max === -1) {
                    $max = $pos;
                    $msg = $com;
                } else {
                    if ($pos < $max) {
                        $max = $pos;
                        $msg = $com;
                    }
                }
            }
        };
        if ($msg === 'SELECT') {
            $re = '/SELECT[\s\S]+FROM\s+(\S+)/m';
            preg_match_all($re, $upper, $matches, PREG_SET_ORDER, 0);
            $msg = 'SELECT ' . $matches[0][1];
        };
        self::stat_count($msg ? $msg : trim(substr(trim($query), 0, 10)));

    }

    public static function stat_str($br = "\n"): string
    {
        $out = '';
        $all = 0;
        foreach (self::$stat['count'] as $msg => $count) {
            $out .= $br . $msg . ':' . $count;
            $all += $count;
        }
        if ($all > 0) {
            $out .= ($out ? $br : '') . 'all:' . $all;
        }

        return $out;
    }

};
