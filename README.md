Net/Gearman
===========

[![Build Status](https://secure.travis-ci.org/mhlavac/gearman.png?branch=master)](http://travis-ci.org/mhlavac/gearman)

PHP library for interfacing with Danga's Gearman. Gearman is a system to farm out work to other machines,
dispatching function calls to machines that are better suited to do work, to do work in parallel, to load
balance lots of function calls, or to call functions between languages. 

Installation
------------

Add following line to your composer.json require
``` json
"mhlavac/gearman": "dev"
``` 

You can use following command
``` sh
composer.phar require --dev mhlavac/geaman:dev
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

This library is under the new BSD license. See the complete license:

    [LICENSE](index.md)

About
-----

I've started working on this because you can't compile PECL gearman extension on windows where i need to use this code.
Goal of this project is to make copy of the PECL gearman extension and allow PHP developers to use this implementation
as a polyfill for it.

Bugs and requests
-----------------

Feel free to report bugs, request a feature or make a pull request. If you want something new in a bundle we would like to know about it.
Make sure that you've checked already opened issues as your bug or feature request might already be in issue list.

