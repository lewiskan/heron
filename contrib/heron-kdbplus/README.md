Heron KDB+
====================

Provides spout implementations for bolt-sink to KDB+. 

### Localhost (32bit)

   * A bolt that opens kdb-plus connection and writes field
   * http://code.kx.com/wiki/Cookbook/InterfacingWithJava
   *
   * Opens connection conn (localhost, port 5001)
   * execute method calls user logic on conn
   *
   * Launch localhost kdb+ listening on port 5001
   * $ q -p 5001