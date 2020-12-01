Net/Gearman
===========

**⚠ This package is abandoned and no longer maintained, please check packagist for alternative Gearman PHP clients ⚠️**

[![Build Status](https://secure.travis-ci.org/mhlavac/gearman.png?branch=master)](http://travis-ci.org/mhlavac/gearman)

PHP library for interfacing with Danga's Gearman. Gearman is a system to farm out work to other machines,
dispatching function calls to machines that are better suited to do work, to do work in parallel, to load
balance lots of function calls, or to call functions between languages. 

Installation
------------

Add following line to your composer.json
``` json
"mhlavac/gearman": "dev"
``` 

You can use following command
``` sh
composer.phar require --dev mhlavac/gearman:dev
```

Examples
--------

### Client

``` php
<?php

$client = new \MHlavac\Gearman\Client();
$client->addServer();

$result = $client->doNormal('replace', 'PHP is best programming language!');
$client->doBackground('long_task', 'PHP rules... PHP rules...');
```

### Worker

``` php
<?php

$function = function($payload) {
    return str_replace('java', 'php', $payload);
};

$worker = new \MHlavac\Gearman\Worker();
$worker->addServer();
$worker->addFunction('replace', $function);

$worker->work();
```

Versioning
----------

This library uses [semantic versioning](http://semver.org/).

License
-------

This library is under the new BSD license. See the complete license. [See the complete license](https://github.com/mhlavac/gearman/blob/master/LICENSE)

About
-----

I've started working on this because you can't compile PECL gearman extension on windows where I had to use the code.
Goal of this project is to make copy of the PECL gearman extension and allow PHP developers to use this implementation
as a polyfill for it.
