mysql-monitor
=============

Monitors a MySQL instance and reports back to Ratchet.io for various performance issues.

Installation
------------

```bash
git clone git@github.com:ratchetio/mysql-monitor.git
cd mysql-monitor

pip install -r requirements.txt
```

Usage
-----

Just pipe your slow query logs into slowqueries.py:

```bash
tail -F /var/log/mysql/mysql-slow.log | python slowqueries.py ACCESS_TOKEN
```

Options
-------

```
Usage: slowqueries.py [options] access_token

Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -e ENVIRONMENT, --environment=ENVIRONMENT
                        The environment in which the mysql instance is
                        running.
  -l NOTIFICATION_LEVEL, --level=NOTIFICATION_LEVEL
                        The minimum level to notify ratchet.io at. Valid
                        values: 0 - debug, 1 - info, 2 - warning, 3 - error, 
                        4 - critical
```
