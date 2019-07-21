<?php


namespace Yonna\Database\Transaction;

use PDOException;

/**
 * 事务
 * Class Transaction
 * @package Yonna\Database\Transaction
 */
class Transaction
{


    /**
     * 多重嵌套事务处理堆栈
     */
    protected $transTrace = 0;


    /**
     * 开始事务
     */
    public function beginTrans()
    {
        if ($this->transTrace <= 0) {
            if ($this->pdo()->inTransaction()) {
                $this->pdo()->commit();
            }
            $this->transTrace = 1;
        } else {
            $this->transTrace++;
            return true;
        }
        try {
            return $this->pdo()->beginTransaction();
        } catch (PDOException $e) {
            // 服务端断开时重连一次
            if ($e->errorInfo[1] == 2006 || $e->errorInfo[1] == 2013) {
                $this->pdoClose();
                return $this->pdo()->beginTransaction();
            } else {
                throw $e;
            }
        }
    }

    /**
     * 提交事务
     */
    public function commitTrans()
    {
        $this->transTrace > 0 && $this->transTrace--;
        if ($this->transTrace > 0) {
            return true;
        }
        return $this->pdo()->commit();
    }

    /**
     * 事务回滚
     */
    public function rollBackTrans()
    {
        $this->transTrace > 0 && $this->transTrace--;
        if ($this->transTrace > 0) {
            return true;
        }
        if ($this->pdo()->inTransaction()) {
            return $this->pdo()->rollBack();
        }
        return false;
    }

    /**
     * 检测是否在一个事务内
     * @return bool
     */
    public function inTransaction()
    {
        return $this->pdo()->inTransaction();
    }

}
