<?php

namespace Yonna\Database\Driver;

use Yonna\Database\Driver\Mdo\Collection;
use Yonna\Throwable\Exception;

class Mongo extends AbstractMDO
{

    /**
     * 构造方法
     *
     * @param array $options
     */
    public function __construct(array $options)
    {
        $options['db_type'] = Type::MONGO;
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
     * @param string $collection
     * @return Collection count
     * @throws Exception\DatabaseException
     */
    public function collection(string $collection): Collection
    {
        if (empty($collection)) {
            Exception::database('collection error');
        }
        $this->options['collection'] = $collection;
        return (new Collection($this->options));
    }

}