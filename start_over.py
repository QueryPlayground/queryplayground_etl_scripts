import rethinkdb as r
import os r.connect( "localhost", 28015).repl()
r.db('queryplayground').table('socrata_datasets').delete().run()
