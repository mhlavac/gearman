<?php
namespace Net\Gearman\Tests;

use Net\Gearman\Worker;

class WorkerTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Worker
     */
    protected $worker;

    public function setUp()
    {
        $this->worker = new Worker();
    }

    public function testAddServer()
    {
        $this->worker->addServer('192.168.1.1');

        $servers = $this->worker->getServers();

        $this->assertCount(1, $servers);
        $this->assertEquals('192.168.1.1:4730', $servers[0]);

        $this->worker->addServer('192.168.1.1', 1234);

        $servers = $this->worker->getServers();

        $this->assertCount(2, $servers);
        $this->assertEquals('192.168.1.1:1234', $servers[1]);

        $this->worker->addServer('example.com');

        $servers = $this->worker->getServers();

        $this->assertCount(3, $servers);
        $this->assertEquals('example.com:4730', $servers[2]);
    }

    public function testAddServerCallWithNoArgumentsAddsLocalhost()
    {
        $this->worker->addServer();

        $servers = $this->worker->getServers();

        $this->assertCount(1, $servers);
        $this->assertEquals('localhost:4730', $servers[0]);
    }

    /**
     * @expectedException \InvalidArgumentException
     */
    public function testAddServerThrowsExceptionIfServerAlreadyExists()
    {
        $this->worker->addServer();
        $this->worker->addServer();
    }

    public function testAddServers()
    {
        $servers = [
            'localhost',
            'localhost:1234',
            'example.com:4730'
        ];

        $this->worker->addServers($servers);

        $servers[0] = 'localhost:4730';

        $this->assertEquals($servers, $this->worker->getServers());
    }

    /**
     * @expectedException \InvalidArgumentException
     */
    public function testAddServersThrowsExceptionIfServerAlreadyExists()
    {
        $servers = [
            'localhost:4730'
        ];

        $this->worker
            ->addServer('localhost')
            ->addServers($servers)
        ;
    }

    public function testAddFunction()
    {
        $gearmanFunctionName = 'reverse';
        $callback = function ($job) {
            return $job->workload();
        };

        $this->worker->addFunction($gearmanFunctionName, $callback);

        $expectedFunctions = [
            $gearmanFunctionName => [
                'callback' => $callback
            ]
        ];

        $this->assertEquals($expectedFunctions, $this->worker->getFunctions());
    }

    /**
     * @expectedException \InvalidArgumentException
     */
    public function testAddFunctionThrowsExceptionIfFunctionIsAlreadyRegistered()
    {
        $this->worker->addFunction('gearmanFunction', 'echo');
        $this->worker->addFunction('gearmanFunction', 'var_dump');
    }

    public function testUnregister()
    {
        $gearmanFunctionName = 'reverse';
        $gearmanFunctionNameSecond = 'reverse2';
        $callback = function ($job) {
            return $job->workload();
        };

        $timeout = 10;
        $this->worker
            ->addFunction($gearmanFunctionName, $callback)
            ->addFunction($gearmanFunctionNameSecond, $callback, $timeout)
        ;

        $this->assertCount(2, $this->worker->getFunctions());

        $this->worker->unregister($gearmanFunctionName);
        $expectedFunctions = [
            $gearmanFunctionNameSecond => [
                'callback' => $callback,
                'timeout' => $timeout
            ]
        ];

        $this->assertCount(1, $this->worker->getFunctions());
        $this->assertEquals($expectedFunctions, $this->worker->getFunctions());
    }

    /**
     * @expectedException \InvalidArgumentException
     */
    public function testUnregisterThrowsExceptionIfFunctionDoesNotExist()
    {
        $this->worker->unregister('gearmanFunction');
    }

    public function testUnregisterAll()
    {
        $gearmanFunctionName = 'reverse';
        $gearmanFunctionNameSecond = 'reverse2';

        $this->worker->addFunction($gearmanFunctionName, 'echo');
        $this->worker->addFunction($gearmanFunctionNameSecond, 'echo');

        $this->assertCount(2, $this->worker->getFunctions());

        $this->worker->unregisterAll();

        $this->assertCount(0, $this->worker->getFunctions());
    }

    /*public function testWorker()
    {
        $function = function($payload) {
            $arg = str_replace('java', 'php', $arg);

            return [$arg];
        };

        $worker = new Worker();
        $worker->addServer();
        $worker->addFunction('replace', $function);

        $worker->work();
    }*/
}
