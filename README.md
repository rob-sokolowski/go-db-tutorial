A toy database following this [blog](https://cstack.github.io/db_tutorial/), [repo](https://github.com/cstack/db_tutorial/tree/master/_parts)

TODO:

[] refactor table to be a redblacktree
	[x] update the value when a new val is inserted


[] Create an SSTable [interface?] that can:
	[] look up the value associated with a key
	[] iterate over all key/val pairs in a specific range
	[] maintain an order for keys
	[] insert new keys
	[] replace values for existing keys

