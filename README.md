# RedisLSMTS
This project is inspired from LSM data structure implemented on Redis for Timeseries database. 
1. It is a NPM module which has CRUD operations as functions which can help you compose this into you own project or built on top of it.
2. It can be hosted by multiple process talking to the same redis for High Availibility.
3. Redis has constrainted space, we solve that with `purge` event to save your data on slower memory devices(SSD/HDD)
4. Its blazing fast cause its on Redis with LSM data structure.




