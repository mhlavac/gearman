<?php

if (!include __DIR__ . '/../vendor/autoload.php') {
    throw new \LogicException('
        You must install all dependencies. Did you run following commands?
        wget http://getcomposer.org/composer.phar
        php composer.phar install
    ');
}
