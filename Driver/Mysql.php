<?php
/**
 * 数据库连接类，依赖 PDO_MYSQL 扩展
 * mysql version >= 5.7
 */

namespace Yonna\Database\Driver;

use Yonna\Database\Driver\Pdo\Table;

class Mysql extends AbstractPDO
{

    public function __construct(array $options)
    {
        $options['db_type'] = Type::MYSQL;
        $options['charset'] = $options['charset'] ?: 'utf8mb4';
        $options['select_sql'] = 'SELECT%DISTINCT% %FIELD% FROM %TABLE% %ALIA% %FORCE%%JOIN%%WHERE%%GROUP%%HAVING%%ORDER%%LIMIT% %LOCK%%COMMENT%';
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
     * @param $table
     * @return Table|null
     */
    public function table(string $table): Table
    {
        $table = str_replace([' as ', ' AS ', ' As ', ' aS ', ' => '], ' ', trim($table));
        $tableEX = explode(' ', $table);
        if (count($tableEX) === 2) {
            $this->options['table'] = $tableEX[1];
            $this->options['table_origin'] = $tableEX[0];
            if (!isset($this->options['alia'])) {
                $this->options['alia'] = array();
            }
            $this->options['alia'][$tableEX[1]] = $tableEX[0];
        } else {
            $this->options['table'] = $table;
            $this->options['table_origin'] = null;
        }
        return new Table($this->options);
    }

}
