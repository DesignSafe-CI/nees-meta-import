nees-meta-import
===================

Walker scripts to add/update NEES files into Agave metadata or Elasticsearch

---

## Setup

    $ ssh username@wrangler.tacc.utexas.edu
    $ cd $HOME
    $ mkdir oracle
    $ cd oracle
    $

Download:
1. [instantclient-basic-linux.x64-12.1.0.2.0.zip](http://www.oracle.com/technetwork/topics/linuxx86-64soft-092277.html)
2. [instantclient-sdk-linux.x64-12.1.0.1.0.zip](http://www.oracle.com/technetwork/topics/linuxx86-64soft-092277.html)

Note: You have to Accept Oracle's License Agreement. I downloaded from browser and scp the zip files to /oracle  

    $ unzip /instantclient-basic-linux.x64-12.1.0.2.0.zip
    $ unzip ../instantclient-sdk-linux.x64-12.1.0.2.0.zip
    $ cd instantclient_12_1/sdk/
    $ unzip ottclasses.zip
    $ cd ..
    $ cp -R ./sdk/* .
    $ cp -R ./sdk/include/* .
    $ ln -s libclntsh.so.12.1 libclntsh.so
    $ ln -s libocci.so.12.1 libocci.so
    $ export LD_LIBRARY_PATH=$HOME/oracle/instantclient_12_1:$LD_LIBRARY_PATH
    $ export PATH=$HOME/oracle/instantclient_12_1:$PATH
    $ cd $WORK
    $ git https://username@github.com:DesignSafe-CI/nees-meta-import.git
    $ cd nees-meta-import/elastic
Edit config.properties [nees-central], [nees-neeshub], [agave], [es]

---

## Run
Setup virtual environment

    $ module load python (skip if mac)
    $ source virtualenvwrapper.sh (skip if mac)
    $ mkvirtualenv dsimport
    $ pip install cx_Oracle
    $ pip install elasticsearch
    $ pip install MySQL-python

To run and index a single NEES-####-####.groups directory:

    $ cd /corral-repl/tacc/NHERI/public/projects
    $ python $WORK/nees-meta-import/elastic/metaes.py NEES-####.####.groups

Note: Don't include trailing ```/``` in ```NEES-####-####.groups``` directory name.

To run and index all directories with a single job:

    $ cd $WORK/nees-meta-import/elastic
    $ run.sh

Note: You must have a current reservation in wrangler to run the job (see ```--reservation=NHERI``` in run.sh) and make sure that  ```/corral-repl/tacc/NHERI/public/projects``` is mounted. To run and index files in agave metadata, repeat steps above but use ```nees-meta-import/agave``` direcotory.

Known errors importing libraries:

MySQLdb Error:

    >>> import MySQLdb
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "/opt/apps/intel15/python/2.7.9/lib/python2.7/site-packages/MySQLdb/__init__.py", line 19, in <module>
        import _mysql
    ImportError: libmysqlclient_r.so.16: cannot open shared object file: No such file or directory

Solution:

    $ module load python
    $ unset PYTHONPATH
    $ source virtualenvwrapper.sh
    $ mkvirtualenv dsimport
    $ pip install MySQL-python
