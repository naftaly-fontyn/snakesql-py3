# SnakeSQL-py3

## Credits
This is a for of the SnakeSQL from https://github.com/elgamar/snakesql-py2.7 that forked 
http://www.pythonweb.org/projects/snakesql/

## General
Why port SnakeSQL?
1. Because it a nice challenge
2. I was looking for SQL like interface to a simple data structure

## License
As the original code is GPL 2 I have to honer and cant change its so it is GPL 2.

## Changelog
2023-06-28: Initial port that passes the db_api2 test

## TODO
- [ ] Write tests using the test module
- [ ] Formate per PEP-8 the code
- [ ] restyle code to be more Pythonic  “Ask forgiveness, not permission” (there are to many `if` test in the code convert to `try/except`)
- [ ] Replace old `%` format strings withe `format` method anf `f""`
- [ ] Convert print statements to loggings
- [ ] Try to eliminate globals/outer scope reference of variables
- [ ] Eliminate use of keywords as variables
- [ ] `driver\base.py` is to long, refractor
- [ ] Add `CREATE TABLE IF NOT EXISTS <table>`