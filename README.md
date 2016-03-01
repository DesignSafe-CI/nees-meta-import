nees-meta-import
===================

Walker scripts to add/update NEES files into Agave metadata or Elasticsearch

---

## Setup

    $ ssh username@wrangler.tacc.utexas.edu
    $ cd $HOME
    $ mkdir oracle
    $ cd oracle

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
    $ export ORACLE_HOME=$HOME/oracle/instantclient_12_1/
    $ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME
    $ cd $WORK
    $ git https://username@github.com:DesignSafe-CI/nees-meta-import.git
    $ cd nees-meta-import/elastic
Edit config.properties [agave], [nees]

---

## Run

To run and index a single NEES-####-####.groups directory:

    $ cd /corral-repl/tacc/NHERI/public/projects
    $ python $WORK/nees-meta-import/elastic/metaes.py NEES-####.####.groups

Note: Don't include trailing ```/``` in ```NEES-####-####.groups``` directory name.

To run and index all directories with a single job:

    $ cd $WORK/nees-meta-import/elastic
    $ run.sh

Note: To run and index files in agave metadata, repeat steps above but use ```nees-meta-import/agave``` direcotory.
