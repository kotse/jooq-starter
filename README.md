# JOOQ Starter with Spring Boot

## Set up

Building via gradle. Initial configuration assumes the following:
- PostgreSQL database running on localhost
- Database named jooq, with user jooq and password jooq

A Vagrant machine with db and relevant db configuration can be found in db/ folder.
To use it install Vagrant and VirtualBox and run `vagrant up` in the folder.

## Database management

Project uses Flyway for migrations. Plugin will assume localhost, db named jooq and username/password jooq. Change that in build.gradle if needed.

- Run migrations - `gradle flywayMigrate`
- Get migrations info - `gradle flywayInfo`
- Clean up database - `gradle flywayClean`