<?php

namespace Yonna\Database\Driver;

use Yonna\Database\Support\Record;
use Yonna\Throwable\Exception;

abstract class AbstractDB
{
    /**
     * 排序类型设置
     */
    const DESC = 'desc';
    const ASC = 'asc';

    /**
     * 数据库驱动类型
     * @var string|null
     */
    protected $db_type = null;

    /**
     * 项目key，用于区分不同项目的缓存key
     * @var mixed|string|null
     */
    protected $project_key = null;

    protected $master = [];
    protected $slave = [];
    protected $host = [];
    protected $port = [];
    protected $account = [];
    protected $password = [];
    protected $name = null;
    protected $replica = null;
    protected $charset = null;
    protected $auto_cache = null;
    protected $auto_crypto = null;
    protected $crypto_type = null;
    protected $crypto_secret = null;
    protected $crypto_iv = null;

    /**
     * action statement select|show|update|insert|delete
     *
     * @var $statement
     */
    protected $statement;

    /**
     * action statetype read|write
     *
     * @var $statetype
     */
    protected $statetype;

    /**
     * where条件对象，实现闭包
     * @var array
     */
    protected $where = array();


    /**
     * 错误信息
     * @var string
     */
    private $error = null;

    /**
     * 是否不执行命令直接返回命令串
     *
     * @var string
     */
    protected $fetchQuery = false;

    /**
     * 加密对象
     * @var Crypto
     */
    protected $Crypto = null;

    /**
     * 是否对内容加密
     * @var bool
     */
    private $is_crypto = false;

    /**
     * 最后请求的链接
     * @var null
     */
    private $last_connection = null;

    /**
     * 构造方法
     *
     * @param array $setting
     * @throws null
     */
    public function __construct(array $setting)
    {
        $this->project_key = $setting['project_key'] ?? null;
        $this->host = $setting['host'] ? explode(',', $setting['host']) : [];
        $this->port = $setting['port'] ? explode(',', $setting['port']) : [];
        $this->account = $setting['account'] ? explode(',', $setting['account']) : [];
        $this->password = $setting['password'] ? explode(',', $setting['password']) : [];
        $this->name = $setting['name'] ?? null;
        $this->replica = $setting['replica'] ?? null;
        $this->charset = $setting['charset'] ?? 'utf8';
        $this->auto_cache = $setting['auto_cache'] ?? false;
        $this->auto_crypto = $setting['auto_crypto'] ?? false;
        $this->crypto_type = $setting['crypto_type'] ?? null;
        $this->crypto_secret = $setting['crypto_secret'] ?? null;
        $this->crypto_iv = $setting['crypto_iv'] ?? null;
        $this->fetchQuery = false;
        $this->Crypto = new Crypto($this->crypto_type, $this->crypto_secret, $this->crypto_iv);
        $this->analysis();
        return $this;
    }

    /**
     * 分析 DSN，设定 master-slave
     * @throws null
     */
    private function analysis()
    {
        if (empty($this->db_type)) {
            Exception::database('Dsn type is Empty');
        }
        // 空数据处理
        for ($i = 0; $i < count($this->host); $i++) {
            if (empty($this->host[$i])) $this->host[$i] = '';
            if (empty($this->port[$i])) $this->port[$i] = '';
            if (empty($this->account[$i])) $this->account[$i] = '';
            if (empty($this->password[$i])) $this->password[$i] = '';
        }
        // 检查服务器属性
        $this->master = [];
        $this->slave = [];
        $dsn = null;
        for ($i = 0; $i < count($this->host); $i++) {
            $conf = [
                'dsn' => $dsn,
                'db_type' => $this->db_type,
                'host' => $this->host[$i],
                'port' => $this->port[$i],
                'account' => $this->account[$i],
                'password' => $this->password[$i],
                'charset' => $this->charset,
            ];
            switch ($this->db_type) {
                case Type::MYSQL:
                    // mysql自动根据关系设定主从库
                    $conf['dsn'] = "mysql:dbname={$this->name};host={$this->host[$i]};port={$this->port[$i]}";
                    $prepare = Malloc::allocation($conf)->prepare("show slave status");
                    $res = $prepare->execute();
                    if ($res) {
                        $slaveStatus = $prepare->fetchAll(\PDO::FETCH_ASSOC);
                        if (!$slaveStatus) {
                            if ($this->master) {
                                Exception::database('master should unique');
                            }
                            $this->master = $conf;
                        } else {
                            $this->slave[] = $conf;
                        }
                    }
                    break;
                case Type::PGSQL:
                    // pgsql自动根据关系设定主从库
                    $conf['dsn'] = "pgsql:dbname={$this->name};host={$this->host[$i]};port={$this->port[$i]}";
                    $prepare = Malloc::allocation($conf)->prepare("SELECT pg_is_in_recovery()");
                    $res = $prepare->execute();
                    if ($res) {
                        $pgStatus = $prepare->fetch(\PDO::FETCH_ASSOC);
                        $pgIsInRecovery = $pgStatus['pg_is_in_recovery'] ?? false;
                        if ($pgIsInRecovery === false) {
                            if ($this->master) {
                                Exception::database('master should unique');
                            }
                            $this->master = $conf;
                        } else {
                            $this->slave[] = $conf;
                        }
                    }
                    break;
                case Type::MSSQL:
                    // mssql取第一个配置为主，后续为从
                    $conf['dsn'] = "sqlsrv:Server={$this->host[$i]},{$this->port[$i]};src={$this->name}";
                    if ($i === 0) {
                        $this->master = $conf;
                    } else {
                        $this->slave[] = $conf;
                    }
                    break;
                case Type::SQLITE:
                    // sqlite取第一个配置为主，后续为从
                    $conf['dsn'] = "sqlite:{$this->host[$i]}" . DIRECTORY_SEPARATOR . $this->name;
                    if ($i === 0) {
                        $this->master = $conf;
                    } else {
                        $this->slave[] = $conf;
                    }
                    break;
                case Type::MONGO:
                    if ($this->account && $this->password) {
                        $conf['dsn'] = "mongodb://{$this->account[$i]}:{$this->password[$i]}@{$this->host[$i]}:{$this->port[$i]}/{$this->name}";
                    } else {
                        $conf['dsn'] = "mongodb://{$this->host[$i]}:{$this->port[$i]}/{$this->name}";
                    }
                    $manager = Malloc::allocation($conf)->getManager();
                    $command = new \MongoDB\Driver\Command(['ping' => 1]);
                    $manager->executeCommand($this->name, $command);
                    $servers = $manager->getServers();
                    /**
                     * @var $server \MongoDB\Driver\Server
                     */
                    $server = reset($servers);
                    if ($server->isPrimary() === true) {
                        $this->master = $conf;
                    } elseif ($server->isSecondary() === true) {
                        $this->slave[] = $conf;
                    }
                    break;
                case Type::REDIS:
                    if (!$this->master) {
                        $conf['dsn'] = "redis://{$this->password[$i]}@{$this->host[$i]}:{$this->port[$i]}";
                        $this->master = $conf;
                    }
                    break;
                case Type::REDIS_CO:
                    if (!$this->master) {
                        $conf['dsn'] = "redisco://{$this->password[$i]}@{$this->host[$i]}:{$this->port[$i]}";
                        $this->master = $conf;
                    }
                    break;
                default:
                    Exception::database("{$this->db_type} type is not supported for the time being");
                    break;
            }
        }
    }

    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        $this->resetAll();
    }

    /**
     * 清除所有数据
     */
    protected function resetAll()
    {
        $this->is_crypto = false;
        $this->error = null;
        $this->where = array();
    }

    /**
     * 设置执行状态
     * @param $statement
     * @return $this
     * @throws Exception\DatabaseException
     */
    protected function setState($statement)
    {
        $this->statement = $statement;
        if ($this->statement === "select" || $this->statement === "show") {
            $this->statetype = "read";
        } elseif ($this->statement === 'update'
            || $this->statement === 'delete'
            || $this->statement === 'insert'
            || $this->statement == 'truncate'
            || $this->statement == 'create') {
            $this->statetype = "write";
        } else {
            Exception::database('Statement Error: ' . $statement);
        }
        return $this;
    }

    /**
     * 是否单例数据库服务
     * @return bool
     */
    protected function isSingleServer()
    {
        return count($this->slave) === 0;
    }

    /**
     * 寻连接池
     * @param bool $force_new
     * @return mixed
     * @throws null
     */
    protected function malloc(bool $force_new = false)
    {
        switch ($this->db_type) {
            case TYPE::MYSQL:
            case TYPE::PGSQL:
            case TYPE::MSSQL:
            case TYPE::SQLITE:
                // pdo的单例/主从
                if ($this->statetype === "write" or $this->isSingleServer()) {
                    $params = $this->master;
                } else if (count($this->slave) === 1) {
                    $params = $this->slave[0];
                } else {
                    $params = $this->slave[random_int(0, count($this->slave) - 1)];
                }
                break;
            case TYPE::MONGO:
                // mongo的单例/副本集
                if ($this->isSingleServer()) {
                    $params = $this->master;
                } else {
                    if (!$this->replica) {
                        Exception::database('Mongo replicaSet not replica config');
                    }
                    $params = $this->master;
                    $params['dsn'] = "mongodb://{$this->master['account']}:{$this->master['password']}@";
                    $params['dsn'] .= "{$this->master['host']}:{$this->master['port']}";
                    foreach ($this->slave as $k => $v) {
                        $params['dsn'] .= ",{$v['host']}:{$v['port']}";
                        $params['host'] .= ",{$v['host']}";
                        $params['port'] .= ",{$v['port']}";
                    }
                    $params['dsn'] .= "/{$this->name}?replicaSet=" . $this->replica;
                }
                break;
            case TYPE::REDIS:
            case TYPE::REDIS_CO:
                // redis暂不支持只选用master
                $params = $this->master;
                break;
        }
        $this->last_connection = $params['dsn'] ?? null;
        if ($force_new) {
            return Malloc::newAllocation($params);
        }
        return Malloc::allocation($params);
    }

    /**
     * 数据库错误信息
     * @param $err
     * @return bool
     */
    protected function error($err)
    {
        $this->error = $err;
        return false;
    }

    /**
     * 获取数据库错误信息
     * @return mixed
     */
    protected function getError()
    {
        return $this->error;
    }


    /**
     * @return bool
     */
    protected function isCrypto(): bool
    {
        return $this->is_crypto;
    }

    /**
     * @tips 一旦设为加密则只能全字而无法模糊匹配
     * @param bool $is_crypto
     * @return AbstractDB|Mysql|Pgsql|Mssql|Sqlite|Mongo|Redis
     */
    protected function enCrypto(bool $is_crypto)
    {
        $this->is_crypto = $is_crypto;
        return $this;
    }

    /**
     * @tips 请求接口
     * @param string $query
     */
    protected function query(string $query)
    {
        Record::add($this->db_type, $this->last_connection, $query);
    }

    /**
     * 设定为直接输出sql
     */
    public function fetchQuery()
    {
        $this->fetchQuery = true;
        return $this;
    }

    /**
     * @param array $whereSet
     * @param array $whereData
     * @return $this
     */
    public function where(array $whereSet, array $whereData)
    {
        foreach ($whereSet as $target => $actions) {
            switch ($this->db_type) {
                case Type::MONGO:
                    $this->whereCollection($target);
                    break;
                default:
                    $this->whereTable($target);
                    break;
            }
            foreach ($actions as $action) {
                foreach ($whereSet as $field) {
                    if (!isset($whereData[$field]) || $whereData[$field] === null) {
                        continue;
                    }
                    if ($whereData[$field] !== null) {
                        switch ($action) {
                            case 'like':
                                $this->$action('%' . $whereData[$field] . '%');
                                break;
                            default:
                                $this->$action($whereData[$field]);
                                break;
                        }
                        $this->$action($whereData[$field]);
                    }
                }
            }
        }
        return $this;
    }

}
