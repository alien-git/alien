CREATE database gold;
GRANT ALL PRIVILEGES ON gold.* TO 'golduser'@'muxito.cern.ch';
FLUSH PRIVILEGES;
GRANT ALL PRIVILEGES ON gold.* TO 'golduser'@'muxito';
FLUSH PRIVILEGES;
