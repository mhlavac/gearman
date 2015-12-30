<?php
namespace MHlavac\Gearman\tests;

use MHlavac\Gearman\Client;
use MHlavac\Gearman\Connection;
use MHlavac\Gearman\Exception;
use Symfony\Component\Process\Process;

class ClientTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Client
     */
    private $client;

    public function setUp()
    {
        $this->client = new Client();
        $this->client->addServer();
    }

    public function testClient()
    {
        $process = new Process("gearman -w -f replace -- sed 's/__replace__/the best/g'");
        $process->start();
        try {
            $this->assertEquals('php is the best', $this->client->doNormal('replace', 'php is __replace__'));
            $this->assertEquals('php is the best', $this->client->doLow('replace', 'php is __replace__'));
            $this->assertEquals('php is the best', $this->client->doHigh('replace', 'php is __replace__'));
            $this->client->doBackground('replace', 'php is __replace__');
            $this->client->doHighBackground('replace', 'php is __replace__');
            $this->client->doLowBackground('replace', 'php is __replace__');
            $tasks = $this->client->doBatch('replace', [6 => 'php is __replace__', 7 => 'php is really __replace__']);
            $this->assertEquals('php is the best', $tasks[6]->result);
            $this->assertEquals('php is really the best', $tasks[7]->result);
        } catch (Exception\CouldNotConnectException $e) {
            $this->markTestSkipped('Skipped, please start Gearman on port ' . Connection::DEFAULT_PORT . ' to be able to run this test');
        }

        $process->stop();
    }
}
