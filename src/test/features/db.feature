Feature: Database
  As a tester
  In order to test system functionality
  I want to control application's database

  Scenario: Insert and verify table
    Given DB table customer
    And customers:
      | id  | name           | date_acquired | time_created             | comment   |
      | 101 | Acme           | 2017-12-13    | 2017-12-13T12:34:56.789Z | A comment |
      | 102 | First national |               | 2017-12-14T12:34:56.789Z |           |
    And customers:
      | id  |
      | 103 |
    Then there are 3 customers
    And the customers are:
      | id  | name                  | date_acquired | time_created            | comment   |
      | 101 | Acme                  | 2017-12-13    | 2017-12-13 12:34:56.789 | A comment |
      | 102 | First national        |               | 2017-12-14 12:34:56.789 |           |
      | 103 | Default customer name |               | 2016-12-31 23:59:58.123 |           |

  Scenario: Get next value from sequence
    Given DB sequence
    When I get next sequence value
    Then the sequence value is 1
    When I get next sequence value
    Then the sequence value is 2
