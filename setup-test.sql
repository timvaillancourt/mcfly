CREATE DATABASE IF NOT EXISTS `test`;
USE `test`;

DROP TABLE IF EXISTS `test`;
CREATE TABLE `test` (
	id INT,
	firstname VARCHAR(255),
	lastname VARCHAR(255),
	PRIMARY KEY (id)
) ENGINE=InnoDB;

INSERT INTO `test` (id, firstname, lastname) VALUES(1, 'Ned', 'Stark');
INSERT INTO `test` (id, firstname, lastname) VALUES(2, 'Catelyn', 'Stark');
INSERT INTO `test` (id, firstname, lastname) VALUES(3, 'Robb', 'Stark');
INSERT INTO `test` (id, firstname, lastname) VALUES(4, 'Sansa', 'Stark');
INSERT INTO `test` (id, firstname, lastname) VALUES(5, 'Arya', '');

UPDATE `test` SET lastname='Stark' WHERE firstname='Arya';

DELETE FROM `test` WHERE id=1;

ALTER TABLE `test` ENGINE=InnoDB;
