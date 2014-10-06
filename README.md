Thumbtack-DB-Challenge
======================


There are 2 jar files that are provided in this project:

Thumbtack-DB-Challenge.jar
Thumbtack-DB-Challenge-TestSuite.jar

To execute simply do:

   java -jar *.jar

to both of the above files.

This was compiled and tested for Java 1.8.

KEY ASSUMPTIONS
---------------

1.  Single-user client sessions are only supported
      a.  thus, transaction are only meaningful for the user as
          the implement the illusion of a scratchpad.  No locking,
          etc. is thus in place
2.  Data resides in memory at all times and is not persisted ever
