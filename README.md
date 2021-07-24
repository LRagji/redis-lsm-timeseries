# RedisLSMTS
This project is inspired from LSM data structure implemented on Redis for Timeseries database. 
1. It is a NPM module which has CRUD operations as functions which can help you compose this into you own project or built on top of it.
2. It can be hosted by multiple process talking to the same redis for High Availibility.
3. Redis has constrainted space, we solve that with `purge` event to save your data on slower memory devices(SSD/HDD)
4. Its blazing fast cause its on Redis with LSM data structure.

## Built with

1. Authors :heart for Open Source.
2. [shortid](https://www.npmjs.com/package/shortid) for auto generating subscribtion handles.
3. [redis-scripto](https://www.npmjs.com/package/redis-scripto) for handling lua scripts.

## Contributions

1. New ideas/techniques are welcomed.
2. Raise a Pull Request.

## Current Version:
W.I.P(Not released yet)


## License
This project is contrubution to public domain and completely free for use, view [LICENSE.md](/license.md) file for details.



