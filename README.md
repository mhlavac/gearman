Net/Gearman
===========

PHP library for interfacing with Danga's Gearman. Gearman is a system to farm out work to other machines,
dispatching function calls to machines that are better suited to do work, to do work in parallel, to load
balance lots of function calls, or to call functions between languages. 

Installation
------------

Add following line to your composer.json require
``` json
"publero/net_gearman": "1.0.x-dev"
``` 

You can use following command
``` sh
composer.phar require --dev publero/net_geaman:1.0.x
```

Examples
--------

### Client

``` php
<?php

$client = new Net\Gearman\Client('localhost:4730');
$client->someBackgroundJob([
    'userid' => 5555,
    'action' => 'new-comment'
]);
```

### Job

``` php
<?php

class Net_Gearman_Job_someBackgroundJob extends Net\Gearman\Job\CommonJob
{
    public function run($args)
    {
        if (!isset($args['userid']) || !isset($args['action'])) {
            throw new Net\Gearman\Job\JobException('Invalid/Missing arguments');
        }

        // Insert a record or something based on the $args

        return []; // Results are returned to Gearman, except for 
                   // background jobs like this one.
    }
}
```

### Worker

``` php
<?php

$worker = new Net\Gearman\Worker('localhost:4730');
$worker->addAbility('someBackgroundJob');
$worker->beginWork();
```
