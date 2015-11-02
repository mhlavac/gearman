.PHONY: all
all: init_composer
	vendor/bin/php-cs-fixer fix
	vendor/bin/phpunit

.PHONY: init_composer
init_composer: composer.phar
	php composer.phar install --prefer-dist

composer.phar:
	curl -s https://getcomposer.org/installer | php
