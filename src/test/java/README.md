# The Athena Webserver Testing Framework
In our testing framework, we break our tests into two distinct types, Unit and Integration Tests:

## Unit Tests [Denoted by: *Tests.java]
A unit test in our framework has three major tenets associated with it:
#### 1. A Unit Test Must Pass With Every Invocation of Maven.
This means that these tests must be sufficiently small and targeted so that the tests themselves do not result in in
churn or false positives. A false-positive is defined as a test failing (usually due to timing) despite a requirement 
being met in the code.
#### 2. A Unit Test Should Test a Single Unit
Although a relatively obvious statement, it has several ramifications for the organization of a unit test.A unit test 
should validate a single unit of work. More specifically though, it should validate a single path for that 
object. This leads to the notion that a an individual `@Test` method having only a single `assert` 
statement. If also leads to the notion that there should be no `if-statements` in the tests. If you introduce an 
`if-else` construct, the `if-portion` and the `else-portion` should be in two separate `@Test` methods. This is not true 
for integration tests and is one of the signals differentiating a unit test from an integration test.
#### 3. A Unit Test is Stateless
Reliability and reproducibility are key aspects of unit testing and tests. As such, unit tests should be stateless. This 
one of the defining differences between a unit and integration test. If you need state, that implies system resources or
a dependency on some other component which would no longer be a unit of work.
## Integration Tests [Denoted by: *IT.java]
An integration test is designed to do some form of end-to-end testing. These are the tests that can consume some system
resources and depend on multiple components to actually work; however, we should strive for our Integration Tests to be
as self contained as possible though so that they can easily be run from the command-line without a lot of additional 
setup.