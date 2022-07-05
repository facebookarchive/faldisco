
# A command line tool to find sensitive data by aligning values in columns
FalDisco - Functional ALignment DISCOvery helps us find sensitive data by leverages the fact 
that we have examples of sensitive data and can use those examples to detect 
additional sensitive data by aligning rows based on known fields(say userid) and then 
aligning fields using the techniques described below. 


## Requirements
FalDisco requires Python 3.9, and has been tested on macOS and Linux. Due to its use of SQLAlchemy, 
it should work with multiple databases, though it has been tested only with MySQL. The only other dependency is Pandas.


## Building and running FalDisco
Install and configure MySQL.

To build from the command line:

$ ```pip install pandas```\
$ ```pip install sqlalchemy```\
$ ```pip install mysqlclient```\
$ ```pip install pymysql```

To run

$ ```python3.9 faldisco.py  mysql.db mysql.db Db```

Usage:\
```python faldisco.py <ref db.ref table> <target db.target table> <ref_join_keys> [target_join_keys]```

The aligned column details will be added to the file(s) out/`{ref_table}`_to_`{target_table}`*

## How FalDisco works
Functional Alignment relies on three simple components:

- A reference table that contains the data that we are looking for
- Row alignment - a set of keys that help us align the rows in the reference table to the rows in the target table that we are trying to run discovery on (i.e., the join key between the two tables). 
- Field alignment - once we align the rows, we try to align the fields through functional alignment that finds any target_field = f(reference field) where f is a deterministic function, so the same value of a reference field always produces the same value of the target field. In the example above, function is an exact match, but it can actually be any deterministic function. 

For SQL native speakers, you can think of it as\
``` SELECT COUNT(*) AS alignment_count FROM reference_table r JOIN target_table t ON r.alignment_key = t.alignment_key WHERE t.target_field = deterministic_function(r.reference_field)```
## Contributing
See the [CONTRIBUTING](CONTRIBUTING.md) file for how to help out.

## License
FalDisco is MIT licensed, as found in the LICENSE file.
