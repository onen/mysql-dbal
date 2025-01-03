<?php

namespace Amp\Mysql\DBAL;

use Amp\Mysql\MysqlConfig;
use Amp\Mysql\SocketMysqlConnector;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\Connection;
use Throwable;
use function Amp\Mysql\mysqlConnector;

final class MysqlDriver extends Driver\AbstractMySQLDriver
{
    public function connect(array $params): Connection
    {
        $config = new MysqlConfig(
            $params['host'] ?? 'localhost',
            $params['port'] ?? MysqlConfig::DEFAULT_PORT,
            $params['user'] ?? '',
            $params['password'] ?? '',
            $params['dbname'] ?? null,
            null,
            $params['charset'] ?? MysqlConfig::DEFAULT_CHARSET
        );

        $connector = mysqlConnector();
        if (isset($params['unix_socket'])) {
            $connector = new SocketMysqlConnector();
        }

        try {
            return new MysqlConnection($connector->connect($config));
        } catch (Throwable $e) {
            throw MysqlException::new($e);
        }
    }
}
