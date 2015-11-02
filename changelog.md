0.1.0
-----

* Moved this library to mhlavac/gearman namespace
* Used PSR-4 to cleanup directories a lot
* Added PHPUnit dev dependency
* Added php_cs settings to set codestyle across project. Please refer to .php_cs see what the code style is

Old Publero/net_gearman changes

1.0.0
-----

* Whole code is rewritten for PHP 5.3 with use of namespaces
* Added composer.json
* Added LICENSE file
* Updated README file
* Added Travis-CI support
* Worker has almost identical API as 1.1.1 PECL gearman library
* Worker no longer returns json as a result (you can send binary data)
* Client uses same server adding API as 1.1.1 PECL gearman library
* Added PHPUnit tests
