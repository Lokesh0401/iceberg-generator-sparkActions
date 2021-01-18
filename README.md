Iceberg-generator helps users generate Iceberg tables (Iceberg documentation at <https://iceberg.apache.org>) using
self-validating tests that can be easily executed.

### Generating Iceberg tables
* Run `mvn clean install` to build the project and run all tests
* Use `mvn test -Dtest=<Test Name>` can be run to execute a specific test.
The same can be achieved with IDEs as well.
  
### Customizing Table generation
Test-specific variables are defined in the Test file itself, which can be changed to suit your needs.
By default, `generated-tables` directory in the project is used as the root of generated tables.

### Adding new tests
If the test you want to add falls under the theme dictated by the test file, add a new test in the same file.
Else, create a new file and add your logic there.
