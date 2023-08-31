A toy database following this [blog](https://cstack.github.io/db_tutorial/), [repo](https://github.com/cstack/db_tutorial/tree/master/_parts)

TODOS:
[x] def a pager
[x] change new_table() to db_open()
[] db_open calls pager_open()
	[x] opens db file 
	[x] inits page cache as nulls
	[] do we want for now to have it write all the data from file into the table?

[] create get_page()
	[] it should write a page from file to pager.pages

[] select
	[] it should handle a "cache miss" 
	[] it should also write the data from file into the new table????
[] insert
	[] should add a row to table
	[] should increase rowCount++


[] create db_close()
	[] flushes page cache to disk (??)
	[] closes bd file
	[] ~~frees memory for Pager and Table~~

