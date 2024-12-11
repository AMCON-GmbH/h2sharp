# h2sharp

H2Sharp is an Ado.Net wrapper for the H2 Database Engine written in C#.

H2 is a java relational database engine. 

This project compiles the Jar of H2 with IKVM.Net (program to convert Java Byte code to .Net’s CIL). 

This project wraps the resulting library with classes that implement the ADO.Net interface to allow for easy use in .Net projects. 

As a result the entire system is in CIL (.Net). 

## Use it

We don't publish this package. To use it, you can clone and build it yourself. Specify your the needed h2 version in the csproj file.
