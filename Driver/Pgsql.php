<?php
/**
 * 数据库连接类，依赖 PDO_PGSQL 扩展
 * version > 9.7
 */

namespace Yonna\Database\Driver;

use Yonna\Database\Driver\Pdo\Schemas;
use Yonna\Database\Driver\Pdo\Table;
use Yonna\Throwable\Exception;

class Pgsql extends AbstractPDO
{

    /**
     * 构造方法
     *
     * @param array $options
     */
    public function __construct(array $options)
    {
        $options['db_type'] = Type::PGSQL;
        $options['charset'] = $options['charset'] ?: 'utf8';
        $options['select_sql'] = 'SELECT%DISTINCT% %FIELD% FROM %SCHEMAS%.%TABLE% %ALIA% %FORCE%%JOIN%%WHERE%%GROUP%%HAVING%%ORDER%%LIMIT% %UNION%%LOCK%%COMMENT%';
        parent::__construct($options);
    }

    /**
     * @return mixed
     */
    public function now()
    {
        return parent::now();
    }

    /**
     * 哪个模式
     *
     * @param string $schemas
     * @return Schemas
     */
    public function schemas($schemas)
    {
        $this->options['schemas'] = $schemas;
        return (new Schemas($this->options));
    }

    /**
     * 哪个模式
     *
     * @param string $table
     * @return Table
     * @throws null
     */
    public function table($table)
    {
        if (empty($this->options['schemas'])) {
            Exception::database('Must set schemas');
        }
        return $this->schemas($this->options['schemas'])->table($table);
    }

}
